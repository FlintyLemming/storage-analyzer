package server

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/FlintyLemming/storage-analyzer/web"
)

type stub struct {
	called bool
	err    error
}

func (s *stub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.called = true
	_, s.err = io.ReadAll(r.Body)
	w.WriteHeader(http.StatusOK)
}

func newTest(login bool) (http.Handler, *stub, *stub) {
	api, lg := &stub{}, &stub{}
	o := Options{API: api, Static: web.Files}
	if login {
		o.Login = lg
	}
	return Handler(o), api, lg
}

func do(h http.Handler, method, target string, body io.Reader) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, target, body))
	return rec
}

func TestSecurityHeaders(t *testing.T) {
	h, _, _ := newTest(true)
	want := map[string]string{
		"Content-Security-Policy": "default-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
		"X-Frame-Options":         "DENY",
		"X-Content-Type-Options":  "nosniff",
		"Referrer-Policy":         "no-referrer",
	}
	for _, p := range []string{"/", "/app.js", "/api.cgi?action=status", "/nope"} {
		rec := do(h, "GET", p, nil)
		for k, v := range want {
			if got := rec.Header().Get(k); got != v {
				t.Errorf("%s %s = %q, want %q", p, k, got, v)
			}
		}
	}
	rec := do(h, "POST", "/app.js", nil)
	if rec.Code != 405 || rec.Header().Get("X-Frame-Options") != "DENY" {
		t.Errorf("405 response missing headers: %d %v", rec.Code, rec.Header())
	}
}

func TestIndexPlatform(t *testing.T) {
	h, _, _ := newTest(true)
	for _, p := range []string{"/", "/index.html"} {
		rec := do(h, "GET", p, nil)
		if rec.Code != 200 || !strings.Contains(rec.Body.String(), `<html lang="en" data-platform="linux">`) {
			t.Errorf("%s: code %d, body lacks data-platform", p, rec.Code)
		}
		for _, attr := range []string{`href="style.css?v=`, `src="i18n.js?v=`, `src="app.js?v=`} {
			if !strings.Contains(rec.Body.String(), attr) {
				t.Errorf("%s: body lacks cache-busted %s", p, attr)
			}
		}
	}
}

func TestIndexMarkerMissingPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	Handler(Options{API: &stub{}, Static: fstest.MapFS{"index.html": {Data: []byte("<html>")}}})
}

func TestShippedIndexHasMarker(t *testing.T) {
	b, err := fs.ReadFile(web.Files, "index.html")
	if err != nil || !bytes.Contains(b, []byte(`<html lang="en">`)) {
		t.Fatalf("marker missing: %v", err)
	}
}

func TestUnknownPaths404(t *testing.T) {
	h, _, _ := newTest(true)
	for _, p := range []string{"/etc/passwd", "/web/app.js", "/index.html/", "/APP.JS"} {
		if rec := do(h, "GET", p, nil); rec.Code != 404 {
			t.Errorf("%s: %d", p, rec.Code)
		}
	}
}

func TestStaticTypes(t *testing.T) {
	h, _, _ := newTest(true)
	for p, ct := range map[string]string{
		"/":          "text/html; charset=utf-8",
		"/app.js":    "text/javascript; charset=utf-8",
		"/i18n.js":   "text/javascript; charset=utf-8",
		"/style.css": "text/css; charset=utf-8",
	} {
		rec := do(h, "GET", p, nil)
		if rec.Code != 200 || rec.Header().Get("Content-Type") != ct {
			t.Errorf("%s: %d %q", p, rec.Code, rec.Header().Get("Content-Type"))
		}
		if rec.Header().Get("Cache-Control") != "no-cache" {
			t.Errorf("%s: cache-control %q", p, rec.Header().Get("Cache-Control"))
		}
	}
	rec := do(h, "HEAD", "/app.js", nil)
	if rec.Code != 200 || rec.Body.Len() != 0 {
		t.Errorf("HEAD: %d len %d", rec.Code, rec.Body.Len())
	}
	rec = do(h, "POST", "/app.js", nil)
	if rec.Code != 405 || rec.Header().Get("Allow") != "GET, HEAD" {
		t.Errorf("POST: %d allow %q", rec.Code, rec.Header().Get("Allow"))
	}
}

func TestAPIRouting(t *testing.T) {
	h, api, lg := newTest(true)
	do(h, "POST", "/api.cgi?action=login", strings.NewReader("x"))
	if !lg.called || api.called {
		t.Errorf("login: login=%v api=%v", lg.called, api.called)
	}
	lg.called = false
	do(h, "POST", "/api.cgi?action=logout", nil)
	if !lg.called || api.called {
		t.Errorf("logout: login=%v api=%v", lg.called, api.called)
	}
	lg.called = false
	do(h, "GET", "/api.cgi?action=status", nil)
	if lg.called || !api.called {
		t.Errorf("status: login=%v api=%v", lg.called, api.called)
	}

	h, api, _ = newTest(false)
	rec := do(h, "POST", "/api.cgi?action=login", nil)
	if rec.Code != 404 || api.called {
		t.Errorf("nil login: %d api=%v", rec.Code, api.called)
	}
}

func TestAPIBodyLimit(t *testing.T) {
	h, api, _ := newTest(true)
	do(h, "POST", "/api.cgi?action=status", bytes.NewReader(make([]byte, 100<<10)))
	var mbe *http.MaxBytesError
	if !errors.As(api.err, &mbe) {
		t.Fatalf("err = %v, want MaxBytesError", api.err)
	}
}

func TestEmbeddedFiles(t *testing.T) {
	for _, n := range []string{"index.html", "app.js", "i18n.js", "style.css"} {
		if _, err := fs.Stat(web.Files, n); err != nil {
			t.Errorf("%s: %v", n, err)
		}
	}
}

func TestNewServerTimeouts(t *testing.T) {
	s := NewServer(":1234", http.NotFoundHandler())
	if s.Addr != ":1234" || s.ReadHeaderTimeout != 10e9 || s.ReadTimeout != 30e9 ||
		s.WriteTimeout != 60e9 || s.IdleTimeout != 120e9 || s.MaxHeaderBytes != 16<<10 {
		t.Errorf("%+v", s)
	}
}
