//go:build linux && !qnap

package main

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestSpawnReapsChildren(t *testing.T) {
	pid, err := startReaped("/bin/sh", "-c", "exit 0")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		// An unreaped child stays a zombie, and signal 0 still succeeds.
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("child %d not reaped", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestNewAppMakesConfigAbsolute(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config"), []byte("data_dir = "+dir+"/data\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	a, err := newApp(&platformFlags{config: "config"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"--config", filepath.Join(dir, "config")}
	if len(a.childArgs) != 2 || a.childArgs[0] != want[0] || a.childArgs[1] != want[1] {
		t.Fatalf("childArgs = %v, want %v", a.childArgs, want)
	}
}
