//go:build !qnap

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const badConfigProbe = "SA_BAD_CONFIG_PROBE"

// A missing or unreadable config file is a configuration error: exit status
// 78 (EX_CONFIG) and the path in the message.
func TestBadConfigExits78(t *testing.T) {
	if path := os.Getenv(badConfigProbe); path != "" {
		os.Args = []string{os.Args[0], "prune", "--config", path}
		main()
		return
	}
	missing := filepath.Join(t.TempDir(), "no-such-config")
	stdout, stderr, code := runChild(t, badConfigProbe+"="+missing)
	if code != 78 {
		t.Fatalf("exit code %d, want 78\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	if !strings.Contains(stderr, missing) {
		t.Fatalf("stderr does not mention %q:\n%s", missing, stderr)
	}
}
