package auth

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHashVerify(t *testing.T) {
	h, err := Hash("correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	ok, err := Verify(h, "correct horse battery")
	if err != nil || !ok {
		t.Fatalf("Verify right = %v, %v", ok, err)
	}
	ok, err = Verify(h, "wrong horse battery")
	if err != nil || ok {
		t.Fatalf("Verify wrong = %v, %v", ok, err)
	}
}

func TestHashFormat(t *testing.T) {
	a, _ := Hash("correct horse battery")
	b, _ := Hash("correct horse battery")
	if !strings.HasPrefix(a, "$argon2id$v=19$m=19456,t=2,p=1$") {
		t.Fatalf("bad prefix: %s", a)
	}
	if a == b {
		t.Fatal("hashes equal; salt not random")
	}
}

func TestVerifyMalformed(t *testing.T) {
	for _, s := range []string{
		"$argon2id$garbage", "", "plain",
		"$bcrypt$v=19$m=19456,t=2,p=1$AAAA$AAAA",
		"$argon2id$v=18$m=19456,t=2,p=1$AAAA$AAAA",
		"$argon2id$v=19$m=99999999,t=2,p=1$AAAA$AAAA",
		"$argon2id$v=19$m=19456,t=2,p=1$!!$AAAA",
	} {
		if _, err := Verify(s, "x"); err == nil {
			t.Errorf("Verify(%q) = nil error", s)
		}
	}
}

func TestCheckPolicy(t *testing.T) {
	if err := CheckPolicy("12345678901"); !errors.Is(err, ErrTooShort) {
		t.Errorf("11 chars: %v", err)
	}
	if err := CheckPolicy("123456789012"); err != nil {
		t.Errorf("12 chars: %v", err)
	}
	if err := CheckPolicy(strings.Repeat("密", 12)); err != nil {
		t.Errorf("12 CJK: %v", err)
	}
}

func TestFingerprint(t *testing.T) {
	if len(Fingerprint("x")) != 32 || Fingerprint("x") == Fingerprint("y") {
		t.Fatal("bad fingerprint")
	}
}

func TestPasswordFileReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "password")
	if err := WritePasswordFile(path, "A-hash"); err != nil {
		t.Fatal(err)
	}
	p := &PasswordFile{Path: path}
	if got, err := p.Current(); err != nil || got != "A-hash" {
		t.Fatalf("got %q, %v", got, err)
	}
	if err := WritePasswordFile(path, "B-hash-longer"); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatal(err)
	}
	if got, err := p.Current(); err != nil || got != "B-hash-longer" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestPasswordFileTrimsNewline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "password")
	os.WriteFile(path, []byte("abc\n"), 0o640)
	got, err := (&PasswordFile{Path: path}).Current()
	if err != nil || got != "abc" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestPasswordFileMissing(t *testing.T) {
	p := &PasswordFile{Path: filepath.Join(t.TempDir(), "nope")}
	if _, err := p.Current(); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("err = %v", err)
	}
}

func TestWritePasswordFileMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "password")
	if err := WritePasswordFile(path, "x"); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o640 {
		t.Fatalf("mode = %v", st.Mode().Perm())
	}
}
