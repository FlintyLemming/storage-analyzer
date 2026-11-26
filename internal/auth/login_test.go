package auth

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/FlintyLemming/storage-analyzer/internal/api"
)

const testPW = "correct horse battery"

func newTestLogin(t *testing.T) (*Login, string) {
	t.Helper()
	dir := t.TempDir()
	phc, err := Hash(testPW)
	if err != nil {
		t.Fatal(err)
	}
	pwPath := filepath.Join(dir, "password")
	if err := WritePasswordFile(pwPath, phc); err != nil {
		t.Fatal(err)
	}
	s, err := OpenSessions(filepath.Join(dir, "sessions.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	l := NewLogin(&PasswordFile{Path: pwPath}, s, NewLimiter(), nil)
	l.FailDelay = 0
	return l, pwPath
}

func doLogin(l *Login, pw string, mod func(*http.Request)) *httptest.ResponseRecorder {
	body, _ := json.Marshal(map[string]string{"password": pw})
	return doRaw(l, "login", http.MethodPost, body, true, mod)
}

func doRaw(l *Login, action, method string, body []byte, hdr bool, mod func(*http.Request)) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "/api.cgi?action="+action, bytes.NewReader(body))
	r.RemoteAddr = "192.0.2.1:1000"
	if hdr {
		r.Header.Set("X-SA-Request", "1")
	}
	if mod != nil {
		mod(r)
	}
	w := httptest.NewRecorder()
	l.ServeHTTP(w, r)
	return w
}

func errCode(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var v struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil {
		t.Fatalf("body %q: %v", w.Body.String(), err)
	}
	return v.Error
}

func sessionCookie(t *testing.T, w *httptest.ResponseRecorder) *http.Cookie {
	t.Helper()
	for _, c := range w.Result().Cookies() {
		if c.Name == "sa_session" {
			return c
		}
	}
	t.Fatal("no sa_session cookie")
	return nil
}

func withCookie(c *http.Cookie) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/api.cgi?action=x", nil)
	r.AddCookie(c)
	return r
}

func TestLoginSuccessSetsCookie(t *testing.T) {
	l, _ := newTestLogin(t)
	w := doLogin(l, testPW, nil)
	if w.Code != 200 || strings.TrimSpace(w.Body.String()) != `{"ok":true}` {
		t.Fatalf("code %d body %q", w.Code, w.Body.String())
	}
	c := sessionCookie(t, w)
	if !c.HttpOnly || c.SameSite != http.SameSiteStrictMode || c.Path != "/" || c.MaxAge != 2592000 || c.Secure {
		t.Fatalf("cookie %+v", c)
	}
	u, admin, err := l.Check(withCookie(c))
	if u != "admin" || !admin || err != nil {
		t.Fatalf("Check = %q %v %v", u, admin, err)
	}
}

func TestLoginSecureCookieOverTLS(t *testing.T) {
	l, _ := newTestLogin(t)
	w := doLogin(l, testPW, func(r *http.Request) { r.TLS = &tls.ConnectionState{} })
	if c := sessionCookie(t, w); !c.Secure {
		t.Fatal("cookie should be Secure")
	}
}

func TestLoginWrongPassword(t *testing.T) {
	l, _ := newTestLogin(t)
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)
	w := doLogin(l, "wrong password!!", nil)
	if w.Code != 401 || errCode(t, w) != "bad_password" {
		t.Fatalf("code %d body %q", w.Code, w.Body.String())
	}
	if len(w.Result().Header.Values("Set-Cookie")) != 0 {
		t.Fatal("unexpected Set-Cookie")
	}
	if !strings.Contains(buf.String(), "login failed from 192.0.2.1") {
		t.Fatalf("log %q", buf.String())
	}
}

func TestLoginLockout(t *testing.T) {
	l, _ := newTestLogin(t)
	log.SetOutput(&bytes.Buffer{})
	defer log.SetOutput(os.Stderr)
	for i := 0; i < 5; i++ {
		if w := doLogin(l, "wrong password!!", nil); w.Code != 401 {
			t.Fatalf("attempt %d: %d", i, w.Code)
		}
	}
	w := doLogin(l, testPW, nil)
	if w.Code != 429 || errCode(t, w) != "too_many_attempts" {
		t.Fatalf("code %d body %q", w.Code, w.Body.String())
	}
	ra := w.Header().Get("Retry-After")
	if ra == "" {
		t.Fatal("no Retry-After")
	}
	var v struct {
		RetryAfter int `json:"retryAfter"`
	}
	json.Unmarshal(w.Body.Bytes(), &v)
	if v.RetryAfter < 1 || ra != strings.TrimSpace(itoa(v.RetryAfter)) {
		t.Fatalf("Retry-After %q retryAfter %d", ra, v.RetryAfter)
	}
}

func itoa(n int) string { b, _ := json.Marshal(n); return string(b) }

func TestLoginMethodAndHeader(t *testing.T) {
	l, _ := newTestLogin(t)
	if w := doRaw(l, "login", http.MethodGet, nil, true, nil); w.Code != 405 || errCode(t, w) != "method_not_allowed" {
		t.Fatalf("GET: %d %q", w.Code, w.Body.String())
	}
	if w := doRaw(l, "login", http.MethodPost, []byte(`{"password":"x"}`), false, nil); w.Code != 403 || errCode(t, w) != "bad_request_origin" {
		t.Fatalf("no header: %d %q", w.Code, w.Body.String())
	}
}

func TestLoginBodyTooLarge(t *testing.T) {
	l, _ := newTestLogin(t)
	big := []byte(`{"password":"` + strings.Repeat("a", 2048) + `"}`)
	if w := doRaw(l, "login", http.MethodPost, big, true, nil); w.Code != 400 || errCode(t, w) != "bad_request" {
		t.Fatalf("%d %q", w.Code, w.Body.String())
	}
	if w := doRaw(l, "login", http.MethodPost, []byte(`{"password":5}`), true, nil); w.Code != 400 {
		t.Fatalf("non-string: %d", w.Code)
	}
}

func TestLogout(t *testing.T) {
	l, _ := newTestLogin(t)
	c := sessionCookie(t, doLogin(l, testPW, nil))
	w := doRaw(l, "logout", http.MethodPost, nil, true, func(r *http.Request) { r.AddCookie(c) })
	if w.Code != 200 {
		t.Fatalf("code %d", w.Code)
	}
	if cc := sessionCookie(t, w); cc.MaxAge >= 0 {
		t.Fatalf("cookie not cleared: %+v", cc)
	}
	if _, _, err := l.Check(withCookie(c)); !errors.Is(err, api.ErrNotLoggedIn) {
		t.Fatalf("err = %v", err)
	}
	// Idempotent without a cookie, but still needs the header.
	if w := doRaw(l, "logout", http.MethodPost, nil, true, nil); w.Code != 200 {
		t.Fatalf("no cookie: %d", w.Code)
	}
	if w := doRaw(l, "logout", http.MethodPost, nil, false, nil); w.Code != 403 {
		t.Fatalf("no header: %d", w.Code)
	}
}

func TestPasswordChangeInvalidatesSessions(t *testing.T) {
	l, pwPath := newTestLogin(t)
	c := sessionCookie(t, doLogin(l, testPW, nil))
	phc, _ := Hash("another long password")
	if err := WritePasswordFile(pwPath, phc); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(2 * time.Second)
	os.Chtimes(pwPath, future, future)
	if _, _, err := l.Check(withCookie(c)); !errors.Is(err, api.ErrNotLoggedIn) {
		t.Fatalf("err = %v", err)
	}
	if w := doLogin(l, "another long password", nil); w.Code != 200 {
		t.Fatalf("new password login: %d", w.Code)
	}
}

func TestCheckNoCookie(t *testing.T) {
	l, _ := newTestLogin(t)
	r := httptest.NewRequest(http.MethodGet, "/api.cgi", nil)
	if _, _, err := l.Check(r); !errors.Is(err, api.ErrNotLoggedIn) {
		t.Fatalf("err = %v", err)
	}
}

func TestLoginUnknownAction(t *testing.T) {
	l, _ := newTestLogin(t)
	if w := doRaw(l, "nope", http.MethodPost, nil, true, nil); w.Code != 404 {
		t.Fatalf("%d", w.Code)
	}
}
