// Package config parses the generic Linux build's config file: simple
// "key = value" lines, whole-line "#" comments, no inline comments.
package config

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
)

// DefaultPath is where the config file lives unless overridden.
const DefaultPath = "/etc/storage-analyzer/config"

// Config holds the parsed settings.
type Config struct {
	Listen         string // default ":8099"
	TLSCert        string
	TLSKey         string
	TrustedProxies []netip.Prefix // single addresses stored as /32 or /128
	DataDir        string         // default "/var/lib/storage-analyzer"
	Paths          []string       // nil = auto-discover
	Exclude        []string
	Dir            string // directory of the config file (set by Load)
}

// PasswordPath returns the password file next to the config file.
func (c Config) PasswordPath() string { return filepath.Join(c.Dir, "password") }

// Load reads and parses the config file at path and sets Dir.
func Load(path string) (Config, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return Config{}, fmt.Errorf("config %s: %w", path, err)
	}
	f, err := os.Open(abs)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()
	c, err := Parse(f)
	if err != nil {
		return Config{}, fmt.Errorf("%s: %w", abs, err)
	}
	c.Dir = filepath.Dir(abs)
	return c, nil
}

// Parse parses config text. Dir is left empty.
func Parse(r io.Reader) (Config, error) {
	c := Config{Listen: ":8099", DataDir: "/var/lib/storage-analyzer"}
	seen := map[string]bool{}
	certLine, keyLine := 0, 0

	sc := bufio.NewScanner(r)
	n := 0
	for sc.Scan() {
		n++
		line := sc.Text()
		if n == 1 {
			line = strings.TrimPrefix(line, "\xef\xbb\xbf")
		}
		line = strings.TrimSpace(line) // also drops a trailing \r
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			return Config{}, fmt.Errorf("config line %d: expected key = value", n)
		}
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if seen[k] {
			return Config{}, fmt.Errorf("config line %d: duplicate key %q", n, k)
		}
		seen[k] = true

		var err error
		switch k {
		case "listen":
			if _, _, err = net.SplitHostPort(v); err != nil {
				err = fmt.Errorf("invalid listen address %q: %w", v, err)
			}
			c.Listen = v
		case "tls_cert":
			c.TLSCert, err = absPath(k, v)
			certLine = n
		case "tls_key":
			c.TLSKey, err = absPath(k, v)
			keyLine = n
		case "data_dir":
			c.DataDir, err = absPath(k, v)
		case "trusted_proxies":
			c.TrustedProxies, err = parseProxies(v)
		case "paths":
			if v == "auto" {
				c.Paths = nil
			} else {
				c.Paths, err = absList(k, v)
			}
		case "exclude":
			c.Exclude, err = absList(k, v)
		default:
			err = fmt.Errorf("unknown key %q", k)
		}
		if err != nil {
			return Config{}, fmt.Errorf("config line %d: %w", n, err)
		}
	}
	if err := sc.Err(); err != nil {
		return Config{}, err
	}
	if certLine != 0 && keyLine == 0 {
		return Config{}, fmt.Errorf("config line %d: tls_cert requires tls_key", certLine)
	}
	if keyLine != 0 && certLine == 0 {
		return Config{}, fmt.Errorf("config line %d: tls_key requires tls_cert", keyLine)
	}
	return c, nil
}

func absPath(key, v string) (string, error) {
	if v == "" {
		return "", fmt.Errorf("%s: empty value", key)
	}
	if !filepath.IsAbs(v) {
		return "", fmt.Errorf("%s: path %q must be absolute", key, v)
	}
	return filepath.Clean(v), nil
}

func splitList(key, v string) ([]string, error) {
	parts := strings.Split(v, ",")
	out := make([]string, len(parts))
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			return nil, fmt.Errorf("%s: empty list item", key)
		}
		out[i] = p
	}
	return out, nil
}

func absList(key, v string) ([]string, error) {
	items, err := splitList(key, v)
	if err != nil {
		return nil, err
	}
	for i, it := range items {
		if items[i], err = absPath(key, it); err != nil {
			return nil, err
		}
	}
	return items, nil
}

func parseProxies(v string) ([]netip.Prefix, error) {
	items, err := splitList("trusted_proxies", v)
	if err != nil {
		return nil, err
	}
	out := make([]netip.Prefix, 0, len(items))
	for _, it := range items {
		if strings.Contains(it, "/") {
			p, err := netip.ParsePrefix(it)
			if err != nil {
				return nil, fmt.Errorf("trusted_proxies: %w", err)
			}
			out = append(out, p.Masked())
			continue
		}
		a, err := netip.ParseAddr(it)
		if err != nil {
			return nil, fmt.Errorf("trusted_proxies: %w", err)
		}
		a = a.Unmap()
		out = append(out, netip.PrefixFrom(a, a.BitLen()))
	}
	return out, nil
}
