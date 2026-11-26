package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"testing"
)

// runChild re-runs this test binary as a child process that executes only the
// calling test, with env added (KEY=VALUE). The test body detects the child by
// one of those variables and then does the in-process work, typically setting
// os.Args and calling main(), which may call os.Exit.
//
// Some things can only be checked in a fresh process: SQLite reads its temp
// directory once, and main exits the process on errors.
func runChild(t *testing.T, env ...string) (stdout, stderr string, code int) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.v")
	cmd.Env = append(os.Environ(), env...)
	var so, se bytes.Buffer
	cmd.Stdout, cmd.Stderr = &so, &se
	err := cmd.Run()
	var ee *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &ee):
		code = ee.ExitCode()
	default:
		t.Fatalf("run child: %v", err)
	}
	return so.String(), se.String(), code
}
