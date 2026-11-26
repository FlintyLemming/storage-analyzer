package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeCert(t *testing.T, certFile, keyFile string, serial int64) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kb}), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestCertReloader(t *testing.T) {
	dir := t.TempDir()
	cf, kf := filepath.Join(dir, "c.pem"), filepath.Join(dir, "k.pem")
	writeCert(t, cf, kf, 1)
	r := &CertReloader{CertFile: cf, KeyFile: kf}
	c, err := r.GetCertificate(nil)
	if err != nil || c.Leaf.SerialNumber.Int64() != 1 {
		t.Fatalf("A: %v %v", c, err)
	}

	writeCert(t, cf, kf, 2)
	future := time.Now().Add(time.Minute)
	for _, f := range []string{cf, kf} {
		if err := os.Chtimes(f, future, future); err != nil {
			t.Fatal(err)
		}
	}
	c, err = r.GetCertificate(nil)
	if err != nil || c.Leaf.SerialNumber.Int64() != 2 {
		t.Fatalf("B: %v %v", c, err)
	}

	// Broken replacement: keep serving B.
	if err := os.WriteFile(cf, []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	later := future.Add(time.Minute)
	os.Chtimes(cf, later, later)
	c, err = r.GetCertificate(nil)
	if err != nil || c.Leaf.SerialNumber.Int64() != 2 {
		t.Fatalf("fallback: %v %v", c, err)
	}
}

func TestCertReloaderNoCache(t *testing.T) {
	r := &CertReloader{CertFile: "/nonexistent/c", KeyFile: "/nonexistent/k"}
	if _, err := r.GetCertificate(nil); err == nil {
		t.Fatal("expected error")
	}
}

func TestTLSConfig(t *testing.T) {
	cfg := TLSConfig(&CertReloader{})
	if cfg.MinVersion != tls.VersionTLS12 || cfg.GetCertificate == nil {
		t.Errorf("%+v", cfg)
	}
}
