package api

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ErrNotLoggedIn means the request has no valid QTS session.
var ErrNotLoggedIn = errors.New("not logged in")

// Authenticator checks the QTS session of a request.
type Authenticator interface {
	Check(r *http.Request) (user string, admin bool, err error)
}

// DefaultAuthURL is the local QTS session check endpoint (served by thttpd).
const DefaultAuthURL = "http://127.0.0.1:58080/cgi-bin/authLogin.cgi"

// QTSAuth validates the NAS_SID session against authLogin.cgi.
type QTSAuth struct{ URL string }

type authXML struct {
	AuthPassed string `xml:"authPassed"`
	Username   string `xml:"username"`
	IsAdmin    string `xml:"isAdmin"`
}

// Check implements Authenticator.
func (a QTSAuth) Check(r *http.Request) (string, bool, error) {
	sid := ""
	if c, err := r.Cookie("NAS_SID"); err == nil {
		sid = c.Value
	}
	if sid == "" {
		sid = r.URL.Query().Get("sid")
	}
	if sid == "" {
		return "", false, ErrNotLoggedIn
	}
	u := a.URL
	if u == "" {
		u = DefaultAuthURL
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(u + "?sid=" + url.QueryEscape(sid))
	if err != nil {
		return "", false, fmt.Errorf("authLogin: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", false, fmt.Errorf("authLogin returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", false, err
	}
	var x authXML
	if err := xml.Unmarshal(body, &x); err != nil {
		return "", false, fmt.Errorf("invalid authLogin XML: %w", err)
	}
	if strings.TrimSpace(x.AuthPassed) != "1" {
		return "", false, ErrNotLoggedIn
	}
	return strings.TrimSpace(x.Username), strings.TrimSpace(x.IsAdmin) == "1", nil
}

// FnOSAuth authenticates requests forwarded by the fnOS unified gateway over
// the local Unix socket, using the X-Trim-* identity headers injected by
// trim_http_cgi after validating the fnOS session.
type FnOSAuth struct{}

// Check implements Authenticator.
func (FnOSAuth) Check(r *http.Request) (string, bool, error) {
	uid := strings.TrimSpace(r.Header.Get("X-Trim-Userid"))
	user := strings.TrimSpace(r.Header.Get("X-Trim-Username"))
	if uid == "" && user == "" {
		return "", false, ErrNotLoggedIn
	}
	if user == "" {
		user = uid
	}
	isAdmin := strings.TrimSpace(r.Header.Get("X-Trim-Isadmin"))
	admin := strings.EqualFold(isAdmin, "true") || isAdmin == "1"
	return user, admin, nil
}
