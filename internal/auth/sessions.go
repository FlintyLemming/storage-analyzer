package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"log"
	"os"
	"time"

	_ "modernc.org/sqlite"
)

// SessionTTL is how long a login stays valid, counted from login (not sliding).
const SessionTTL = 30 * 24 * time.Hour

// Sessions is a SQLite-backed session store. Only the SHA-256 of each token is
// stored. It is safe for concurrent use.
type Sessions struct{ db *sql.DB }

const sessionsSchema = `CREATE TABLE IF NOT EXISTS sessions(
	hash BLOB PRIMARY KEY,
	created INTEGER NOT NULL,
	expires INTEGER NOT NULL,
	pwfp TEXT NOT NULL
)`

// OpenSessions opens (creating if needed) the session database at path with mode 0600.
func OpenSessions(path string) (*Sessions, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	f.Close()
	if err := os.Chmod(path, 0o600); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(sessionsSchema); err != nil {
		db.Close()
		return nil, err
	}
	return &Sessions{db: db}, nil
}

func hashToken(token string) []byte {
	h := sha256.Sum256([]byte(token))
	return h[:]
}

// Create stores a new session bound to the password fingerprint pwfp and
// returns its token. Expired sessions are purged as a side effect.
func (s *Sessions) Create(pwfp string, now time.Time) (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	token := base64.RawURLEncoding.EncodeToString(b)
	if _, err := s.db.Exec("DELETE FROM sessions WHERE expires <= ?", now.Unix()); err != nil {
		return "", err
	}
	_, err := s.db.Exec("INSERT INTO sessions(hash, created, expires, pwfp) VALUES (?, ?, ?, ?)",
		hashToken(token), now.Unix(), now.Add(SessionTTL).Unix(), pwfp)
	if err != nil {
		return "", err
	}
	return token, nil
}

// Valid reports whether token is a live session issued for password fingerprint pwfp.
func (s *Sessions) Valid(token, pwfp string, now time.Time) bool {
	if len(token) != 43 {
		return false
	}
	var expires int64
	var fp string
	err := s.db.QueryRow("SELECT expires, pwfp FROM sessions WHERE hash = ?", hashToken(token)).Scan(&expires, &fp)
	if err == sql.ErrNoRows {
		return false
	}
	if err != nil {
		log.Printf("auth: session lookup: %v", err)
		return false
	}
	return fp == pwfp && now.Unix() < expires
}

// Delete removes the session for token (no error if absent).
func (s *Sessions) Delete(token string) error {
	_, err := s.db.Exec("DELETE FROM sessions WHERE hash = ?", hashToken(token))
	return err
}

// Close closes the database.
func (s *Sessions) Close() error { return s.db.Close() }
