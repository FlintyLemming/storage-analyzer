//go:build !qnap

package main

import (
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/FlintyLemming/storage-analyzer/internal/auth"
)

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
	_, cfg := writeConfig(t, addr)

	if out, errOut, code := runChild(t, childArgsEnv+"=passwd --config "+cfg, childStdinEnv+"=correct horse battery\n"); code != 0 {
		t.Fatalf("passwd exit %d\n%s\n%s", code, out, errOut)
	}

	// serve keeps running, which runChild (waits for exit) cannot do.
	var stderr strings.Builder
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
