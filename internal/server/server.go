// Package server is the standalone HTTP front end for the generic Linux build.
// It must not be imported by the QNAP CGI build.
package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"net/http"
	"strconv"
	"time"
)

// Options configures Handler.
type Options struct {
	API    http.Handler // handles /api.cgi
	Login  http.Handler // handles action=login/logout; nil returns 404 for them
	Static fs.FS        // frontend files
}

const (
	indexMarker      = `<html lang="en">`
	indexReplacement = `<html lang="en" data-platform="linux">`
	maxAPIBody       = 64 << 10
)

type asset struct {
	body  []byte
	ctype string
}

// Handler builds the server's root handler. It panics if the index page in
// o.Static lacks the expected marker.
func Handler(o Options) http.Handler {
	raw, err := fs.ReadFile(o.Static, "index.html")
	if err != nil {
		panic(fmt.Sprintf("server: read index.html: %v", err))
	}
	if !bytes.Contains(raw, []byte(indexMarker)) {
		panic("server: index.html lacks " + indexMarker)
	}
	index := bytes.Replace(raw, []byte(indexMarker), []byte(indexReplacement), 1)
	for _, ref := range [][2]string{
		{"style.css", `href="style.css"`},
		{"i18n.js", `src="i18n.js"`},
		{"app.js", `src="app.js"`},
	} {
		if b, err := fs.ReadFile(o.Static, ref[0]); err == nil {
			sum := sha256.Sum256(b)
			tag := hex.EncodeToString(sum[:4])
			withHash := ref[1][:len(ref[1])-1] + "?v=" + tag + `"`
			index = bytes.Replace(index, []byte(ref[1]), []byte(withHash), 1)
		}
	}

	h := &handler{o: o, assets: map[string]asset{
		"/":           {index, "text/html; charset=utf-8"},
		"/index.html": {index, "text/html; charset=utf-8"},
	}}
	h.lazy = map[string]string{
		"/app.js":    "text/javascript; charset=utf-8",
		"/i18n.js":   "text/javascript; charset=utf-8",
		"/style.css": "text/css; charset=utf-8",
	}
	return h
}

type handler struct {
	o      Options
	assets map[string]asset
	lazy   map[string]string // served from o.Static on each request
}

func setSecurityHeaders(w http.ResponseWriter) {
	hd := w.Header()
	hd.Set("Content-Security-Policy", "default-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
	hd.Set("X-Frame-Options", "DENY")
	hd.Set("X-Content-Type-Options", "nosniff")
	hd.Set("Referrer-Policy", "no-referrer")
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	setSecurityHeaders(w)
	path := r.URL.Path

	if path == "/api.cgi" {
		h.serveAPI(w, r)
		return
	}

	var a asset
	if v, ok := h.assets[path]; ok {
		a = v
	} else if ct, ok := h.lazy[path]; ok {
		b, err := fs.ReadFile(h.o.Static, path[1:])
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		a = asset{b, ct}
	} else {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", a.ctype)
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Content-Length", strconv.Itoa(len(a.body)))
	if r.Method == http.MethodHead {
		return
	}
	w.Write(a.body)
}

func (h *handler) serveAPI(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxAPIBody)
	switch r.URL.Query().Get("action") {
	case "login", "logout":
		if h.o.Login == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		h.o.Login.ServeHTTP(w, r)
	default:
		h.o.API.ServeHTTP(w, r)
	}
}

// NewServer returns an http.Server with conservative timeouts.
func NewServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
}
