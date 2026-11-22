package main

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const tempProbeRoot = "SA_TEMP_PROBE_ROOT"

// QTS mounts /tmp (and so /var/tmp) as a 64MB tmpfs, too small for the sort
// files SQLite writes while indexing a large duplicate check result.
//
// SQLite picks its temp directory once per process, so the check runs in a
// child: a real command first, then a query that spills to a temp file.
func TestSQLiteTempFilesGoToDataDir(t *testing.T) {
	if root := os.Getenv(tempProbeRoot); root != "" {
		os.Args = []string{os.Args[0], "prune", "--root", root}
		main()
		probeTempFile(t)
		return
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.v")
	cmd.Env = append(os.Environ(), tempProbeRoot+"="+root, "SQLITE_TMPDIR=", "TMPDIR=")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("child: %v\n%s", err, out)
	}
	_, rest, ok := strings.Cut(string(out), "tempfile=")
	if !ok {
		t.Fatalf("child opened no temp file:\n%s", out)
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
