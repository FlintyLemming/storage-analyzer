//go:build !qnap && !fnos

package main

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/FlintyLemming/storage-analyzer/internal/auth"
	"github.com/FlintyLemming/storage-analyzer/internal/catalog"
	"github.com/FlintyLemming/storage-analyzer/internal/runner"
	"github.com/FlintyLemming/storage-analyzer/internal/scandb"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

const (
	childArgsEnv  = "SA_CHILD_ARGS"  // run main with these (space separated) arguments
	childStdinEnv = "SA_CHILD_STDIN" // becomes the child's standard input
)

// childMain makes a re-executed test binary behave as the storage-analyzer
// binary. It reports whether this process is such a child; main may exit.
func childMain(t *testing.T) bool {
	args := os.Getenv(childArgsEnv)
	if args == "" {
		return false
	}
	os.Args = append([]string{os.Args[0]}, strings.Fields(args)...)
	if in, ok := os.LookupEnv(childStdinEnv); ok {
		f, err := os.CreateTemp(t.TempDir(), "stdin")
		if err != nil {
			t.Fatal(err)
		}
		f.WriteString(in)
		f.Seek(0, io.SeekStart)
		os.Stdin = f
	}
	main()
	return true
}

func writeConfig(t *testing.T, listen string) (root, cfg string) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg = filepath.Join(root, "config")
	text := "data_dir = " + filepath.Join(root, "data") + "\n"
	if listen != "" {
		text += "listen = " + listen + "\n"
	}
	if err := os.WriteFile(cfg, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	return root, cfg
}

func TestServeWithoutPasswordExits78(t *testing.T) {
	if childMain(t) {
		return
	}
	_, cfg := writeConfig(t, "127.0.0.1:0")
	_, stderr, code := runChild(t, childArgsEnv+"=serve --config "+cfg)
	if code != 78 {
		t.Fatalf("exit %d, want 78\n%s", code, stderr)
	}
	if !strings.Contains(stderr, "storage-analyzer passwd") {
		t.Fatalf("stderr lacks passwd hint:\n%s", stderr)
	}
}

func TestServeBadPasswordExits78(t *testing.T) {
	if childMain(t) {
		return
	}
	root, cfg := writeConfig(t, "127.0.0.1:0")
	if err := os.WriteFile(filepath.Join(root, "password"), []byte("not-a-phc\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, stderr, code := runChild(t, childArgsEnv+"=serve --config "+cfg)
	if code != 78 {
		t.Fatalf("exit %d, want 78\n%s", code, stderr)
	}
}

func TestServeBadTLSExits78(t *testing.T) {
	if childMain(t) {
		return
	}
	root, cfg := writeConfig(t, "127.0.0.1:0")
	if _, errOut, code := runChild(t, childArgsEnv+"=passwd --config "+cfg, childStdinEnv+"=correct horse battery\n"); code != 0 {
		t.Fatalf("passwd exit %d\n%s", code, errOut)
	}
	f, err := os.OpenFile(cfg, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("tls_cert = " + filepath.Join(root, "missing.pem") + "\ntls_key = " + filepath.Join(root, "missing.key") + "\n")
	f.Close()
	_, stderr, code := runChild(t, childArgsEnv+"=serve --config "+cfg)
	if code != 78 {
		t.Fatalf("exit %d, want 78\n%s", code, stderr)
	}
}

func TestPasswdFromStdin(t *testing.T) {
	if childMain(t) {
		return
	}
	root, cfg := writeConfig(t, "")
	pwPath := filepath.Join(root, "password")
	passwd := func(stdin string) (stdout, stderr string, code int) {
		return runChild(t, childArgsEnv+"=passwd --config "+cfg, childStdinEnv+"="+stdin)
	}

	if out, errOut, code := passwd("short\n"); code == 0 {
		t.Fatalf("short password accepted:\n%s\n%s", out, errOut)
	}
	if _, err := os.Stat(pwPath); err == nil {
		t.Fatal("password file created for rejected password")
	}
	out, errOut, code := passwd("correct horse battery\r\n")
	if code != 0 || !strings.Contains(out, "password updated; all sessions are signed out") {
		t.Fatalf("exit %d\n%s\n%s", code, out, errOut)
	}
	b, err := os.ReadFile(pwPath)
	if err != nil {
		t.Fatal(err)
	}
	ok, err := auth.Verify(strings.TrimSpace(string(b)), "correct horse battery")
	if err != nil || !ok {
		t.Fatalf("Verify = %v, %v", ok, err)
	}
}

func TestServeEndToEnd(t *testing.T) {
	if childMain(t) {
		return
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	root, cfg := writeConfig(t, addr)
	scanDir := filepath.Join(root, "vol")
	if err := os.MkdirAll(scanDir, 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(cfg, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("paths = " + scanDir + "\n")
	f.Close()

	if out, errOut, code := runChild(t, childArgsEnv+"=passwd --config "+cfg, childStdinEnv+"=correct horse battery\n"); code != 0 {
		t.Fatalf("passwd exit %d\n%s\n%s", code, out, errOut)
	}

	// serve keeps running, which runChild (waits for exit) cannot do.
	var stderr syncBuffer
	srv := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
	srv.Env = append(os.Environ(), childArgsEnv+"=serve --config "+cfg)
	srv.Stderr = &stderr
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1) // receives Wait's result once
	done := make(chan struct{})   // closed when the child is gone
	go func() {
		exited <- srv.Wait()
		close(done)
	}()
	t.Cleanup(func() {
		srv.Process.Kill()
		<-done
	})

	deadline := time.Now().Add(15 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		select {
		case err := <-exited:
			t.Fatalf("server exited early: %v\n%s", err, stderr.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("server not listening:\n%s", stderr.String())
		}
		time.Sleep(50 * time.Millisecond)
	}

	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar, Timeout: 10 * time.Second}
	base := "http://" + addr
	do := func(req *http.Request) (*http.Response, string) {
		t.Helper()
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp, string(body)
	}
	get := func(path string) (*http.Response, string) {
		req, _ := http.NewRequest("GET", base+path, nil)
		return do(req)
	}

	resp, _ := get("/")
	if resp.StatusCode != 200 || resp.Header.Get("Content-Security-Policy") == "" {
		t.Fatalf("GET /: %d, CSP %q", resp.StatusCode, resp.Header.Get("Content-Security-Policy"))
	}
	resp, body := get("/api.cgi?action=status")
	if resp.StatusCode != 401 || !strings.Contains(body, `"auth":"password"`) {
		t.Fatalf("status without login: %d %s", resp.StatusCode, body)
	}
	resp, body = get("/api.cgi?action=volumes")
	if resp.StatusCode != 401 || !strings.Contains(body, `"auth":"password"`) {
		t.Fatalf("volumes without login: %d %s", resp.StatusCode, body)
	}
	req, _ := http.NewRequest("POST", base+"/api.cgi?action=login", strings.NewReader(`{"password":"correct horse battery"}`))
	req.Header.Set("X-SA-Request", "1")
	req.Header.Set("Content-Type", "application/json")
	resp, body = do(req)
	if resp.StatusCode != 200 || len(resp.Cookies()) == 0 {
		t.Fatalf("login: %d %s cookies=%v", resp.StatusCode, body, resp.Cookies())
	}
	resp, body = get("/api.cgi?action=status")
	if resp.StatusCode != 200 {
		t.Fatalf("status after login: %d %s", resp.StatusCode, body)
	}
	resp, body = get("/api.cgi?action=volumes")
	if resp.StatusCode != 200 || !strings.Contains(body, scanDir) {
		t.Fatalf("volumes after login: %d %s", resp.StatusCode, body)
	}

	if err := srv.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("exit after SIGTERM: %v\n%s", err, stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no exit within 5s of SIGTERM\n%s", stderr.String())
	}
}

func TestDeleteCommandCLI(t *testing.T) {
	if childMain(t) {
		return
	}
	root, cfg := writeConfig(t, "")
	dataDir := filepath.Join(root, "data")
	volDir := filepath.Join(root, "vol")
	if err := os.MkdirAll(volDir, 0o755); err != nil {
		t.Fatal(err)
	}
	targetFile := filepath.Join(volDir, "trash.bin")
	if err := os.WriteFile(targetFile, []byte("delete-me"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(cfg, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("paths = " + volDir + "\n")
	f.Close()

	// Run a scan via CLI.
	if out, errOut, code := runChild(t, childArgsEnv+"=scan --trigger manual --config "+cfg); code != 0 {
		t.Fatalf("scan exit %d\n%s\n%s", code, out, errOut)
	}
	// Resolve entry ID for trash.bin.
	db, err := scandb.Open(catalog.ScanPath(dataDir, 1))
	if err != nil {
		t.Fatal(err)
	}
	ch, _, err := db.Children(1, scandb.ListOpts{Limit: 10})
	db.Close()
	if err != nil || len(ch) != 1 || string(ch[0].Name) != "trash.bin" {
		t.Fatalf("children = %+v err=%v", ch, err)
	}
	reqPath, err := runner.WriteDeleteRequest(dataDir, runner.DeleteRequest{JobID: "cli-1", ScanID: 1, EntryIDs: []int64{ch[0].ID}})
	if err != nil {
		t.Fatal(err)
	}
	// Run delete via CLI.
	if out, errOut, code := runChild(t, childArgsEnv+"=delete --request "+reqPath+" --config "+cfg); code != 0 {
		t.Fatalf("delete exit %d\n%s\n%s", code, out, errOut)
	}
	if _, err := os.Stat(targetFile); !os.IsNotExist(err) {
		t.Fatalf("trash.bin still exists: %v", err)
	}
	if _, err := os.Stat(reqPath); !os.IsNotExist(err) {
		t.Fatalf("request file was not cleaned up: %v", err)
	}
	last, err := runner.LastDelete(dataDir)
	if err != nil || last == nil || last.ID != "cli-1" || last.Status != "done" {
		t.Fatalf("LastDelete = %+v err=%v", last, err)
	}

	// Verify allow_delete = false refuses CLI delete too.
	f, _ = os.OpenFile(cfg, os.O_APPEND|os.O_WRONLY, 0o600)
	f.WriteString("allow_delete = false\n")
	f.Close()
	reqPath2, _ := runner.WriteDeleteRequest(dataDir, runner.DeleteRequest{JobID: "cli-2", ScanID: 1, EntryIDs: []int64{ch[0].ID}})
	if _, errOut, code := runChild(t, childArgsEnv+"=delete --request "+reqPath2+" --config "+cfg); code == 0 {
		t.Fatalf("delete succeeded despite allow_delete = false\n%s", errOut)
	}
}
