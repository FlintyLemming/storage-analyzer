package auth

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
)

func prefixes(ss ...string) []netip.Prefix {
	var out []netip.Prefix
	for _, s := range ss {
		out = append(out, netip.MustParsePrefix(s))
	}
	return out
}

func TestClientIP(t *testing.T) {
	loop := prefixes("127.0.0.1/32")
	tests := []struct {
		name    string
		remote  string
		trusted []netip.Prefix
		xff     []string
		want    string
	}{
		{"untrusted ignores xff", "203.0.113.9:1234", nil, []string{"1.2.3.4"}, "203.0.113.9"},
		{"rightmost untrusted", "127.0.0.1:5555", loop, []string{"6.6.6.6, 198.51.100.7"}, "198.51.100.7"},
		{"skip trusted", "127.0.0.1:5555", loop, []string{"198.51.100.7, 127.0.0.1"}, "198.51.100.7"},
		{"no xff", "127.0.0.1:5555", loop, nil, "127.0.0.1"},
		{"mapped peer", "[::ffff:10.0.0.1]:1", nil, nil, "10.0.0.1"},
		{"garbage stops", "127.0.0.1:5555", loop, []string{"6.6.6.6, garbage, 127.0.0.1"}, "127.0.0.1"},
		{"multiple lines", "127.0.0.1:5555", loop, []string{"6.6.6.6", "198.51.100.7"}, "198.51.100.7"},
		{"all trusted", "127.0.0.1:5555", loop, []string{"127.0.0.1"}, "127.0.0.1"},
		{"mapped xff entry", "127.0.0.1:5555", loop, []string{"::ffff:127.0.0.1, 9.9.9.9"}, "9.9.9.9"},
		{"no port", "203.0.113.9", nil, nil, "203.0.113.9"},
		{"zoned ipv6 xff stripped", "127.0.0.1:5555", loop, []string{"fe80::1%eth0"}, "fe80::1"},
		{"zoned ipv6 peer trusted", "[fe80::1%eth0]:5555", prefixes("fe80::/64"), []string{"198.51.100.7"}, "198.51.100.7"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/", nil)
			r.RemoteAddr = tc.remote
			for _, v := range tc.xff {
				r.Header.Add("X-Forwarded-For", v)
			}
			if got := ClientIP(r, tc.trusted); got != netip.MustParseAddr(tc.want) {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "nonsense"
	if got := ClientIP(r, loop); got.IsValid() {
		t.Fatalf("expected zero addr, got %v", got)
	}
}

func TestIsHTTPS(t *testing.T) {
	loop := prefixes("127.0.0.1/32")
	mk := func(remote, proto string) *http.Request {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = remote
		if proto != "" {
			r.Header.Set("X-Forwarded-Proto", proto)
		}
		return r
	}
	r := mk("203.0.113.9:1", "")
	r.TLS = &tls.ConnectionState{}
	if !IsHTTPS(r, nil) {
		t.Fatal("TLS should be https")
	}
	if !IsHTTPS(mk("127.0.0.1:1", "HTTPS"), loop) {
		t.Fatal("trusted proxy https")
	}
	if !IsHTTPS(mk("127.0.0.1:1", "https, http"), loop) {
		t.Fatal("first value of list")
	}
	if IsHTTPS(mk("127.0.0.1:1", "http, https"), loop) {
		t.Fatal("first value http")
	}
	if IsHTTPS(mk("203.0.113.9:1", "https"), loop) {
		t.Fatal("untrusted peer must be ignored")
	}
}
