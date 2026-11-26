package auth

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func openTestSessions(t *testing.T) (*Sessions, string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "sessions.db")
	s, err := OpenSessions(p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, p
}

func TestSessionLifecycle(t *testing.T) {
	s, _ := openTestSessions(t)
	now := time.Unix(1_700_000_000, 0)
	tok, err := s.Create("fp1", now)
	if err != nil {
		t.Fatal(err)
	}
	if len(tok) != 43 {
		t.Fatalf("token length %d", len(tok))
	}
	if !s.Valid(tok, "fp1", now) {
		t.Fatal("should be valid")
	}
	if s.Valid(tok, "fp1", now.Add(SessionTTL+time.Second)) {
		t.Fatal("should be expired")
	}
	if s.Valid(tok, "other", now) {
		t.Fatal("different fingerprint should be invalid")
	}
	for _, bad := range []string{"", "x", "!!!"} {
		if s.Valid(bad, "fp1", now) {
			t.Fatalf("malformed %q valid", bad)
		}
	}
	if err := s.Delete(tok); err != nil {
		t.Fatal(err)
	}
	if s.Valid(tok, "fp1", now) {
		t.Fatal("deleted session valid")
	}
}

func TestSessionsPurgeExpired(t *testing.T) {
	s, _ := openTestSessions(t)
	now := time.Unix(1_700_000_000, 0)
	old, _ := s.Create("fp", now)
	if _, err := s.Create("fp", now.Add(SessionTTL+time.Hour)); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM sessions").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("rows = %d, want 1", n)
	}
	_ = old
}

func TestSessionsSurviveReopen(t *testing.T) {
	s, p := openTestSessions(t)
	now := time.Now()
	tok, _ := s.Create("fp", now)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := OpenSessions(p)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if !s2.Valid(tok, "fp", now) {
		t.Fatal("session lost after reopen")
	}
}

func TestSessionsStoreNoRawToken(t *testing.T) {
	s, p := openTestSessions(t)
	tok, _ := s.Create("fp", time.Now())
	s.Close()
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(tok)) {
		t.Fatal("raw token found in db file")
	}
}

func TestSessionsFileMode(t *testing.T) {
	s, p := openTestSessions(t)
	s.Close()
	if err := os.Chmod(p, 0o644); err != nil {
		t.Fatal(err)
	}
	s2, err := OpenSessions(p)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v", fi.Mode().Perm())
	}
}
