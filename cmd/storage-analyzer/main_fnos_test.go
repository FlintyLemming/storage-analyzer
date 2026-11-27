//go:build linux && fnos

package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/FlintyLemming/storage-analyzer/internal/catalog"
	"github.com/FlintyLemming/storage-analyzer/internal/scandb"
)

const (
	fnosChildArgsEnv = "SA_FNOS_CHILD_ARGS"
	fnosTempProbeEnv = "SA_FNOS_TEMP_PROBE_DATA"
)

type syncBuf struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func fnosChildMain(t *testing.T) bool {
	args := os.Getenv(fnosChildArgsEnv)
	if args == "" {
		return false
	}
	os.Args = append([]string{os.Args[0]}, strings.Fields(args)...)
	main()
	return true
}

func TestFnOSConfigParsing(t *testing.T) {
	cfg, err := parseFnOSConfig(strings.NewReader("# comment\ninclude_system = true\ninclude_external = false\nallow_delete = false\n"))
	if err != nil {
		t.Fatalf("parseFnOSConfig: %v", err)
	}
	if !cfg.IncludeSystem || cfg.IncludeExternal || cfg.AllowDelete {
		t.Fatalf("got %+v", cfg)
	}
	if _, err := parseFnOSConfig(strings.NewReader("unknown_key = true\n")); err == nil {
		t.Fatal("expected error on unknown key")
	}
	if _, err := parseFnOSConfig(strings.NewReader("allow_delete = maybe\n")); err == nil {
		t.Fatal("expected error on invalid bool")
	}
	// Missing file falls back to defaults.
	def, err := loadFnOSConfig(filepath.Join(t.TempDir(), "missing.conf"))
	if err != nil {
		t.Fatalf("missing config: %v", err)
	}
	if def.IncludeSystem || def.IncludeExternal || !def.AllowDelete {
		t.Fatalf("default = %+v", def)
	}
}

func TestFnOSSQLiteTempFilesGoToDataDir(t *testing.T) {
	if dataDir := os.Getenv(fnosTempProbeEnv); dataDir != "" {
		os.Args = []string{os.Args[0], "prune", "--data", dataDir}
		main()
		probeTempFileFnOS(t)
		return
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	out, stderr, code := runChild(t, fnosTempProbeEnv+"="+dataDir, "SQLITE_TMPDIR=", "TMPDIR=")
	if code != 0 {
		t.Fatalf("child exit %d\n%s\n%s", code, out, stderr)
	}
	_, rest, ok := strings.Cut(out, "tempfile=")
	if !ok {
		t.Fatalf("child opened no temp file:\n%s\n%s", out, stderr)
	}
	got, _, _ := strings.Cut(rest, "\n")
	if filepath.Dir(got) != dataDir {
		t.Fatalf("SQLite temp file %q, want it in %q", got, dataDir)
	}
}

func probeTempFileFnOS(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "probe.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	for _, q := range []string{
		"PRAGMA temp.cache_size=10",
		"CREATE TEMP TABLE spill(x)",
		"INSERT INTO spill WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<5000) SELECT randomblob(100) FROM c",
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, fd := range fds {
		if p, err := os.Readlink("/proc/self/fd/" + fd.Name()); err == nil && strings.Contains(p, "etilqs_") {
			fmt.Printf("tempfile=%s\n", strings.TrimSuffix(p, " (deleted)"))
			return
		}
	}
}

func TestFnOSServeUnixSocketEndToEnd(t *testing.T) {
	if fnosChildMain(t) {
		return
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(root, "var")
	sockPath := filepath.Join(root, "app.sock")
	cfgPath := filepath.Join(root, "settings.conf")
	scanDir := filepath.Join(root, "vol")
	if err := os.MkdirAll(scanDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(scanDir, "sample.txt"), []byte("hello fnos"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfgPath, []byte("allow_delete = true\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var stderr syncBuf
	srv := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
	srv.Env = append(os.Environ(), fnosChildArgsEnv+"=serve --data "+dataDir+" --config "+cfgPath+" --socket "+sockPath+" --prefix /app/storage-analyzer --paths "+scanDir)
	srv.Stderr = &stderr
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	done := make(chan struct{})
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
		c, err := net.DialTimeout("unix", sockPath, 200*time.Millisecond)
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
			t.Fatalf("socket not ready:\n%s", stderr.String())
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Socket must be 0600 (owner-only).
	st, err := os.Stat(sockPath)
	if err != nil {
		t.Fatal(err)
	}
	if perm := st.Mode().Perm(); perm != 0o600 {
		t.Fatalf("socket mode = %o, want 0600", perm)
	}

	client := &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", sockPath)
			},
		},
	}

	doReq := func(method, path string, hdr map[string]string) (*http.Response, string) {
		t.Helper()
		req, err := http.NewRequest(method, "http://unix"+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp, string(b)
	}

	// Bare gateway prefix redirects to trailing slash.
	resp, _ := doReq("GET", "/app/storage-analyzer", nil)
	if resp.StatusCode != http.StatusMovedPermanently || resp.Header.Get("Location") != "/app/storage-analyzer/" {
		t.Fatalf("bare prefix: %d loc=%q", resp.StatusCode, resp.Header.Get("Location"))
	}

	// Prefixed index serves data-platform="fnos" with SAMEORIGIN iframe header.
	resp, body := doReq("GET", "/app/storage-analyzer/", nil)
	if resp.StatusCode != 200 || !strings.Contains(body, `data-platform="fnos"`) {
		t.Fatalf("index: %d", resp.StatusCode)
	}
	if resp.Header.Get("X-Frame-Options") != "SAMEORIGIN" {
		t.Fatalf("X-Frame-Options = %q", resp.Header.Get("X-Frame-Options"))
	}

	// Without X-Trim headers -> 401.
	resp, body = doReq("GET", "/app/storage-analyzer/api.cgi?action=status", nil)
	if resp.StatusCode != 401 || !strings.Contains(body, `"not_logged_in"`) {
		t.Fatalf("no auth: %d %s", resp.StatusCode, body)
	}

	// Non-admin user -> 403.
	resp, body = doReq("GET", "/app/storage-analyzer/api.cgi?action=status", map[string]string{
		"X-Trim-Userid": "1001", "X-Trim-Username": "bob", "X-Trim-Isadmin": "false",
	})
	if resp.StatusCode != 403 || !strings.Contains(body, `"not_admin"`) {
		t.Fatalf("non-admin: %d %s", resp.StatusCode, body)
	}

	// Admin user -> 200 status & volumes.
	adminHdr := map[string]string{
		"X-Trim-Userid": "1000", "X-Trim-Username": "admin", "X-Trim-Isadmin": "true",
	}
	resp, body = doReq("GET", "/app/storage-analyzer/api.cgi?action=status", adminHdr)
	if resp.StatusCode != 200 || !strings.Contains(body, `"deleteEnabled":true`) {
		t.Fatalf("admin status: %d %s", resp.StatusCode, body)
	}
	resp, body = doReq("GET", "/app/storage-analyzer/api.cgi?action=volumes", adminHdr)
	if resp.StatusCode != 200 || !strings.Contains(body, scanDir) {
		t.Fatalf("admin volumes: %d %s", resp.StatusCode, body)
	}

	// Trigger a scan via CLI with --paths and verify API lists the result.
	if out, errOut, code := runChild(t, fnosChildArgsEnv+"=scan --trigger manual --data "+dataDir+" --config "+cfgPath+" --paths "+scanDir); code != 0 {
		t.Fatalf("scan exit %d\n%s\n%s", code, out, errOut)
	}
	db, err := scandb.Open(catalog.ScanPath(dataDir, 1))
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	resp, body = doReq("GET", "/app/storage-analyzer/api.cgi?action=list&scan=1&dir=1", adminHdr)
	if resp.StatusCode != 200 || !strings.Contains(body, `"sample.txt"`) {
		t.Fatalf("list after scan: %d %s", resp.StatusCode, body)
	}

	// SIGTERM triggers clean shutdown and removes socket.
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
	if _, err := os.Stat(sockPath); !os.IsNotExist(err) {
		t.Fatalf("socket left behind after shutdown: %v", err)
	}
}
