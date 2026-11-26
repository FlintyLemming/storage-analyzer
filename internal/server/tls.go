package server

import (
	"crypto/tls"
	"log"
	"os"
	"sync"
	"time"
)

// CertReloader serves a certificate that is reloaded when either file's
// modification time changes.
type CertReloader struct {
	CertFile, KeyFile string

	mu       sync.Mutex
	cert     *tls.Certificate
	certTime time.Time
	keyTime  time.Time
}

// GetCertificate implements tls.Config.GetCertificate.
func (c *CertReloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ci, err := os.Stat(c.CertFile)
	if err == nil {
		var ki os.FileInfo
		if ki, err = os.Stat(c.KeyFile); err == nil {
			if c.cert != nil && ci.ModTime().Equal(c.certTime) && ki.ModTime().Equal(c.keyTime) {
				return c.cert, nil
			}
			var cert tls.Certificate
			if cert, err = tls.LoadX509KeyPair(c.CertFile, c.KeyFile); err == nil {
				c.cert, c.certTime, c.keyTime = &cert, ci.ModTime(), ki.ModTime()
				return c.cert, nil
			}
		}
	}
	if c.cert != nil {
		log.Printf("tls: reload failed, keeping previous certificate: %v", err)
		return c.cert, nil
	}
	return nil, err
}

// TLSConfig returns a tls.Config using c for certificates.
func TLSConfig(c *CertReloader) *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: c.GetCertificate}
}
