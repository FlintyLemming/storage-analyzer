// Package auth implements the built-in single-password login of the Linux
// build: password hashing, the password file, sessions and rate limiting.
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"golang.org/x/crypto/argon2"
)

// MinPasswordLen is the minimum password length in Unicode characters.
const MinPasswordLen = 12

// ErrTooShort is returned by CheckPolicy for passwords below MinPasswordLen.
var ErrTooShort = errors.New("password must be at least 12 characters")

// Argon2id parameters used for new hashes (OWASP minimum: 19 MiB, t=2, p=1).
const (
	argonMemory  = 19456 // KiB
	argonTime    = 2
	argonThreads = 1
	saltLen      = 16
	keyLen       = 32
)

// Upper bounds on parameters parsed from a stored hash, so a tampered file
// cannot make verification allocate unbounded memory or CPU.
const (
	maxArgonMemory  = 1 << 20 // KiB (1 GiB)
	maxArgonTime    = 16
	maxArgonThreads = 16
	maxSaltLen      = 1024
	maxKeyLen       = 1024
)

// CheckPolicy enforces the password policy.
func CheckPolicy(pw string) error {
	if utf8.RuneCountInString(pw) < MinPasswordLen {
		return ErrTooShort
	}
	return nil
}

// Hash returns the argon2id PHC string of pw with a fresh random salt.
func Hash(pw string) (string, error) {
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	key := argon2.IDKey([]byte(pw), salt, argonTime, argonMemory, argonThreads, keyLen)
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s", argon2.Version,
		argonMemory, argonTime, argonThreads,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(key)), nil
}

var errBadHash = errors.New("malformed password hash")

// Verify reports whether pw matches the PHC string. The parameters are read
// from phc itself, so hashes made with older parameters keep verifying.
func Verify(phc, pw string) (bool, error) {
	parts := strings.Split(phc, "$")
	// "", "argon2id", "v=19", "m=..,t=..,p=..", salt, hash
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" {
		return false, errBadHash
	}
	if parts[2] != "v=19" {
		return false, fmt.Errorf("%w: unsupported version", errBadHash)
	}
	var m, t, p uint64
	ps := strings.Split(parts[3], ",")
	if len(ps) != 3 {
		return false, errBadHash
	}
	for i, pair := range []struct {
		prefix string
		dst    *uint64
	}{{"m=", &m}, {"t=", &t}, {"p=", &p}} {
		s, ok := strings.CutPrefix(ps[i], pair.prefix)
		if !ok {
			return false, errBadHash
		}
		v, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			return false, errBadHash
		}
		*pair.dst = v
	}
	if m == 0 || t == 0 || p == 0 || m > maxArgonMemory || t > maxArgonTime || p > maxArgonThreads {
		return false, fmt.Errorf("%w: parameters out of range", errBadHash)
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil || len(salt) == 0 || len(salt) > maxSaltLen {
		return false, errBadHash
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil || len(want) == 0 || len(want) > maxKeyLen {
		return false, errBadHash
	}
	got := argon2.IDKey([]byte(pw), salt, uint32(t), uint32(m), uint8(p), uint32(len(want)))
	return subtle.ConstantTimeCompare(got, want) == 1, nil
}

// Fingerprint identifies a password hash (first 32 hex chars of its SHA-256);
// it changes whenever the password changes.
func Fingerprint(phc string) string {
	sum := sha256.Sum256([]byte(phc))
	return hex.EncodeToString(sum[:])[:32]
}

// PasswordFile reads the PHC string from a file, re-reading it only when the
// file's identity, mtime or size changed. Safe for concurrent use.
type PasswordFile struct {
	Path string

	mu    sync.Mutex
	info  os.FileInfo
	val   string
	valid bool
}

// Current returns the stored hash. A missing file yields an error matching
// fs.ErrNotExist.
func (p *PasswordFile) Current() (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	st, err := os.Stat(p.Path)
	if err != nil {
		p.valid = false
		return "", err
	}
	if p.valid && os.SameFile(p.info, st) && st.ModTime().Equal(p.info.ModTime()) && st.Size() == p.info.Size() {
		return p.val, nil
	}
	b, err := os.ReadFile(p.Path)
	if err != nil {
		p.valid = false
		return "", err
	}
	p.val = strings.TrimRight(string(b), "\r\n")
	p.info, p.valid = st, true
	return p.val, nil
}

// WritePasswordFile atomically replaces path with phc (mode 0640). When run
// as root and the group "storage-analyzer" exists, the file becomes
// root:storage-analyzer.
func WritePasswordFile(path, phc string) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".password-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp) // no-op after a successful rename
	if _, err := f.WriteString(phc + "\n"); err != nil {
		f.Close()
		return err
	}
	if err := f.Chmod(0o640); err != nil {
		f.Close()
		return err
	}
	if os.Geteuid() == 0 {
		if g, err := user.LookupGroup("storage-analyzer"); err == nil {
			if gid, err := strconv.Atoi(g.Gid); err == nil {
				if err := f.Chown(0, gid); err != nil {
					f.Close()
					return err
				}
			}
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
