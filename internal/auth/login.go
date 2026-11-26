package auth

import (
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log"
	"math"
	"net/http"
	"net/netip"
	"strconv"
	"time"

	"github.com/FlintyLemming/storage-analyzer/internal/api"
)

// CookieName is the session cookie name.
const CookieName = "sa_session"

// maxLoginBody is the upper bound on the login request body.
const maxLoginBody = 1 << 10

// Login serves login and logout and authenticates API requests by session
// cookie. It implements api.Authenticator.
type Login struct {
	Password  *PasswordFile
	Sessions  *Sessions
	Limiter   *Limiter
	Trusted   []netip.Prefix
	Now       func() time.Time
	FailDelay time.Duration

	sem chan struct{} // capacity 1: one password hash check at a time
}

// NewLogin returns a Login with Now = time.Now and a one second fail delay.
func NewLogin(pw *PasswordFile, s *Sessions, l *Limiter, trusted []netip.Prefix) *Login {
	return &Login{
		Password:  pw,
		Sessions:  s,
		Limiter:   l,
		Trusted:   trusted,
		Now:       time.Now,
		FailDelay: time.Second,
		sem:       make(chan struct{}, 1),
	}
}

var _ api.Authenticator = (*Login)(nil)

// Check implements api.Authenticator. The session must have been created
// for the current password, so changing the password logs everyone out.
func (l *Login) Check(r *http.Request) (string, bool, error) {
	c, err := r.Cookie(CookieName)
	if err != nil || c.Value == "" {
		return "", false, api.ErrNotLoggedIn
	}
	phc, err := l.Password.Current()
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", false, api.ErrNotLoggedIn
		}
		log.Printf("auth: read password file: %v", err)
		return "", false, errors.New("authentication unavailable")
	}
	if !l.Sessions.Valid(c.Value, Fingerprint(phc), l.Now()) {
		return "", false, api.ErrNotLoggedIn
	}
	return "admin", true, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]string{"error": code, "message": msg})
}

// ServeHTTP dispatches ?action=login and ?action=logout.
func (l *Login) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Query().Get("action") {
	case "login":
		l.login(w, r)
	case "logout":
		l.logout(w, r)
	default:
		writeErr(w, http.StatusNotFound, "unknown_action", "unknown action")
	}
}

// precheck enforces POST and the CSRF header; it reports whether to go on.
func precheck(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeErr(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return false
	}
	if r.Header.Get("X-SA-Request") != "1" {
		writeErr(w, http.StatusForbidden, "bad_request_origin", "missing X-SA-Request header")
		return false
	}
	return true
}

func (l *Login) login(w http.ResponseWriter, r *http.Request) {
	if !precheck(w, r) {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxLoginBody+1))
	var req struct {
		Password *string `json:"password"`
	}
	if err != nil || len(body) > maxLoginBody || json.Unmarshal(body, &req) != nil || req.Password == nil {
		writeErr(w, http.StatusBadRequest, "bad_request", "expected {\"password\": string} of at most 1 KiB")
		return
	}

	now := l.Now()
	ip := ClientIP(r, l.Trusted)

	// Allow, verification and Fail/Succeed all happen under the semaphore, so
	// a parallel burst cannot get past the lockout before failures are counted.
	select {
	case l.sem <- struct{}{}:
	case <-r.Context().Done():
		return
	}
	if retry, ok := l.Limiter.Allow(ip, now); !ok {
		<-l.sem
		secs := int(math.Ceil(retry.Seconds()))
		if secs < 1 {
			secs = 1
		}
		w.Header().Set("Retry-After", strconv.Itoa(secs))
		writeJSON(w, http.StatusTooManyRequests, map[string]any{
			"error":      "too_many_attempts",
			"message":    "too many failed attempts, try again later",
			"retryAfter": secs,
		})
		return
	}
	phc, err := l.Password.Current()
	var ok bool
	if err == nil {
		ok, err = Verify(phc, *req.Password)
	}
	if err != nil {
		<-l.sem
		log.Printf("login: %v", err)
		writeErr(w, http.StatusInternalServerError, "internal", "internal error")
		return
	}
	if !ok {
		l.Limiter.Fail(ip, now)
	} else {
		l.Limiter.Succeed(ip)
	}
	<-l.sem

	if !ok {
		log.Printf("login failed from %s", ip)
		if l.FailDelay > 0 {
			t := time.NewTimer(l.FailDelay)
			select {
			case <-t.C:
			case <-r.Context().Done():
				t.Stop()
				return
			}
		}
		writeErr(w, http.StatusUnauthorized, "bad_password", "wrong password")
		return
	}

	tok, err := l.Sessions.Create(Fingerprint(phc), now)
	if err != nil {
		log.Printf("login: create session: %v", err)
		writeErr(w, http.StatusInternalServerError, "internal", "internal error")
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     CookieName,
		Value:    tok,
		Path:     "/",
		MaxAge:   int(SessionTTL / time.Second),
		HttpOnly: true,
		Secure:   IsHTTPS(r, l.Trusted),
		SameSite: http.SameSiteStrictMode,
	})
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (l *Login) logout(w http.ResponseWriter, r *http.Request) {
	if !precheck(w, r) {
		return
	}
	if c, err := r.Cookie(CookieName); err == nil && c.Value != "" {
		if err := l.Sessions.Delete(c.Value); err != nil {
			log.Printf("logout: %v", err)
		}
	}
	http.SetCookie(w, &http.Cookie{
		Name:     CookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   IsHTTPS(r, l.Trusted),
		SameSite: http.SameSiteStrictMode,
	})
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
