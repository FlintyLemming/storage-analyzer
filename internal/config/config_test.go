package config

import (
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func pfx(s string) netip.Prefix { return netip.MustParsePrefix(s) }

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    Config
		wantErr string // substring; empty = success
	}{
		{
			name: "empty",
			in:   "",
			want: Config{Listen: ":8099", DataDir: "/var/lib/storage-analyzer"},
		},
		{
			name: "spec example",
			in: `# /etc/storage-analyzer/config
listen = :8099
tls_cert = /etc/storage-analyzer/cert.pem
tls_key  = /etc/storage-analyzer/key.pem
trusted_proxies = 127.0.0.1, ::1
data_dir = /var/lib/storage-analyzer
paths = auto
exclude = /boot/efi, /mnt/backup
`,
			want: Config{
				Listen:         ":8099",
				TLSCert:        "/etc/storage-analyzer/cert.pem",
				TLSKey:         "/etc/storage-analyzer/key.pem",
				TrustedProxies: []netip.Prefix{pfx("127.0.0.1/32"), pfx("::1/128")},
				DataDir:        "/var/lib/storage-analyzer",
				Exclude:        []string{"/boot/efi", "/mnt/backup"},
			},
		},
		{
			name: "paths list cleaned",
			in:   "paths = /, /srv/data/\n",
			want: Config{Listen: ":8099", DataDir: "/var/lib/storage-analyzer", Paths: []string{"/", "/srv/data"}},
		},
		{
			name: "trusted proxies",
			in:   "trusted_proxies = 127.0.0.1, ::1, 10.0.0.0/8\n",
			want: Config{Listen: ":8099", DataDir: "/var/lib/storage-analyzer",
				TrustedProxies: []netip.Prefix{pfx("127.0.0.1/32"), pfx("::1/128"), pfx("10.0.0.0/8")}},
		},
		{
			name: "trusted proxy cidr masked",
			in:   "trusted_proxies = 10.1.2.3/8\n",
			want: Config{Listen: ":8099", DataDir: "/var/lib/storage-analyzer",
				TrustedProxies: []netip.Prefix{pfx("10.0.0.0/8")}},
		},
		{
			name: "crlf bom spaces comments blanks",
			in:   "\xef\xbb\xbf# comment\r\n\r\n   listen   =   127.0.0.1:9000  \r\n  # another\r\ndata_dir=/data\r\n",
			want: Config{Listen: "127.0.0.1:9000", DataDir: "/data"},
		},
		{name: "unknown key", in: "\nlistn = :1\n", wantErr: `line 2: unknown key "listn"`},
		{name: "duplicate key", in: "listen = :1\nlisten = :2\n", wantErr: "line 2"},
		{name: "no equals", in: "listen = :1\nfoo\n", wantErr: "line 2"},
		{name: "relative paths", in: "paths = a/b\n", wantErr: "line 1"},
		{name: "relative exclude", in: "exclude = /a, b\n", wantErr: "line 1"},
		{name: "relative data_dir", in: "data_dir = x\n", wantErr: "line 1"},
		{name: "relative tls_cert", in: "tls_cert = c.pem\ntls_key = /k\n", wantErr: "line 1"},
		{name: "relative tls_key", in: "tls_cert = /c\ntls_key = k.pem\n", wantErr: "line 2"},
		{name: "only tls_cert", in: "tls_cert = /c\n", wantErr: "tls_key"},
		{name: "only tls_key", in: "\ntls_key = /k\n", wantErr: "tls_cert"},
		{name: "bad listen", in: "listen = 8099\n", wantErr: "line 1"},
		{name: "bad proxy", in: "trusted_proxies = abc\n", wantErr: "line 1"},
		{name: "empty list item", in: "exclude = /a,,/b\n", wantErr: "line 1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Parse(strings.NewReader(tc.in))
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("want error containing %q, got nil", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %+v\nwant %+v", got, tc.want)
			}
		})
	}
}

func TestLoadSetsDir(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "config")
	if err := os.WriteFile(p, []byte("listen = :1234\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != ":1234" {
		t.Fatalf("Listen = %q", c.Listen)
	}
	if got, want := c.PasswordPath(), filepath.Join(dir, "password"); got != want {
		t.Fatalf("PasswordPath = %q, want %q", got, want)
	}
	if _, err := Load(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("want error for missing file")
	}
}
