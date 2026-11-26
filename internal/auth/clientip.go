package auth

import (
	"net/http"
	"net/netip"
	"strings"
)

func isTrusted(a netip.Addr, trusted []netip.Prefix) bool {
	a = a.Unmap().WithZone("")
	for _, p := range trusted {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

func peerAddr(r *http.Request) netip.Addr {
	if ap, err := netip.ParseAddrPort(r.RemoteAddr); err == nil {
		return ap.Addr().Unmap().WithZone("")
	}
	if a, err := netip.ParseAddr(r.RemoteAddr); err == nil {
		return a.Unmap().WithZone("")
	}
	return netip.Addr{}
}

// ClientIP returns the originating address of the request. X-Forwarded-For is
// honoured only when the direct peer is a trusted proxy, and is walked from
// right to left so that entries a client could forge are never believed.
func ClientIP(r *http.Request, trusted []netip.Prefix) netip.Addr {
	peer := peerAddr(r)
	if !peer.IsValid() || !isTrusted(peer, trusted) {
		return peer
	}
	var entries []string
	for _, line := range r.Header.Values("X-Forwarded-For") {
		for _, e := range strings.Split(line, ",") {
			entries = append(entries, strings.TrimSpace(e))
		}
	}
	for i := len(entries) - 1; i >= 0; i-- {
		a, err := netip.ParseAddr(entries[i])
		if err != nil {
			return peer
		}
		a = a.Unmap().WithZone("")
		if isTrusted(a, trusted) {
			continue
		}
		return a
	}
	return peer
}

// IsHTTPS reports whether the client connection is HTTPS, either directly or
// via a trusted proxy that says so in X-Forwarded-Proto.
func IsHTTPS(r *http.Request, trusted []netip.Prefix) bool {
	if r.TLS != nil {
		return true
	}
	peer := peerAddr(r)
	if !peer.IsValid() || !isTrusted(peer, trusted) {
		return false
	}
	v := r.Header.Get("X-Forwarded-Proto")
	if i := strings.IndexByte(v, ','); i >= 0 {
		v = v[:i]
	}
	return strings.EqualFold(strings.TrimSpace(v), "https")
}
