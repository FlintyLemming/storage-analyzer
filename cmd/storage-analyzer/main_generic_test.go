//go:build linux && !qnap

package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const tempProbeRoot = "SA_TEMP_PROBE_ROOT"

// SQLite picks its temp directory once per process, so the check runs in a
// child: a real command first, then a query that spills to a temp file. The
// temp files must go to the configured data directory, not /tmp or /var/tmp.
func TestSQLiteTempFilesGoToDataDir(t *testing.T) {
	if root := os.Getenv(tempProbeRoot); root != "" {
		os.Args = []string{os.Args[0], "prune", "--config", filepath.Join(root, "config")}
		main()
		probeTempFile(t)
		return
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := "data_dir = " + filepath.Join(root, "data") + "\n"
	if err := os.WriteFile(filepath.Join(root, "config"), []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	out, stderr, code := runChild(t, tempProbeRoot+"="+root, "SQLITE_TMPDIR=", "TMPDIR=")
	if code != 0 {
		t.Fatalf("child exit %d\n%s\n%s", code, out, stderr)
	}
	_, rest, ok := strings.Cut(out, "tempfile=")
	if !ok {
		t.Fatalf("child opened no temp file:\n%s\n%s", out, stderr)
	}
	got, _, _ := strings.Cut(rest, "\n")
	if want := filepath.Join(root, "data"); filepath.Dir(got) != want {
		t.Fatalf("SQLite temp file %q, want it in %q", got, want)
	}
}

// probeTempFile makes SQLite spill and prints the file it spilled to.
func probeTempFile(t *testing.T) {
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
