package api

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// The scan must survive the CGI request: it has to run in its own session.
func TestSpawnDetachedNewSession(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "pid")
	if err := spawnDetached("/bin/sh", "-c", "echo $$ > "+pidFile+"; sleep 2"); err != nil {
		t.Fatal(err)
	}
	var pid int
	for i := 0; i < 50 && pid == 0; i++ {
		time.Sleep(20 * time.Millisecond)
		b, _ := os.ReadFile(pidFile)
		pid, _ = strconv.Atoi(strings.TrimSpace(string(b)))
	}
	if pid == 0 {
		t.Fatal("child never ran")
	}
	sid, err := unix.Getsid(pid)
	if err != nil {
		t.Fatal(err)
	}
	if sid != pid {
		t.Fatalf("child session = %d, want its own session %d", sid, pid)
	}
	if mine, _ := unix.Getsid(0); sid == mine {
		t.Fatal("child shares the parent's session")
	}
}
