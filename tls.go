package redis_cache

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// buildTLSConfig assembles a *tls.Config from the parsed tls* directives.
// Returns (nil, nil) when TLS is not enabled (the caller leaves the redis client's
// TLSConfig field unset, yielding a plaintext connection).
//
// Verification matrix (defaults: both flags true ⇒ standard full verification):
//
//	tlsVerifyChain=true,  tlsVerifyHostname=true   → standard TLS verification.
//	tlsVerifyChain=true,  tlsVerifyHostname=false  → trust the chain, ignore hostname mismatch
//	                                                  (multi-host topologies where each peer has
//	                                                  its own cert signed by the same CA).
//	tlsVerifyChain=false                           → no verification at all (skip everything;
//	                                                  hostname check is implicitly off).
func (re *Redis) buildTLSConfig() (*tls.Config, error) {
	if !re.tlsEnabled {
		return nil, nil
	}

	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if re.tlsCert != "" || re.tlsKey != "" {
		if re.tlsCert == "" || re.tlsKey == "" {
			return nil, fmt.Errorf("tls client certificate and key must both be provided")
		}
		cert, err := tls.LoadX509KeyPair(re.tlsCert, re.tlsKey)
		if err != nil {
			return nil, fmt.Errorf("load tls client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	if re.tlsCA != "" {
		pem, err := os.ReadFile(re.tlsCA)
		if err != nil {
			return nil, fmt.Errorf("read tls ca file %q: %w", re.tlsCA, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no valid PEM certificates found in tls ca file %q", re.tlsCA)
		}
		cfg.RootCAs = pool
	}
	// If RootCAs is nil here, Go's TLS stack falls back to the operating system's
	// trust store — that's the default "use OS CA bundle" branch.

	switch {
	case !re.tlsVerifyChain:
		// Disable everything. This is the dev / explicit-trust-nothing escape hatch.
		cfg.InsecureSkipVerify = true //nolint:gosec // user opt-in via tls_verify_chain off

	case !re.tlsVerifyHostname:
		// Trust the chain but ignore hostname mismatch — common when one client connects
		// to multiple peers (cluster seeds, sentinel quorum, read replicas) whose certs
		// share a CA but differ in SAN/CN. Go's tls.Config has no native flag for this,
		// so we disable the built-in verifier and run the chain verification ourselves
		// without the DNSName option.
		cfg.InsecureSkipVerify = true //nolint:gosec // chain still validated below
		roots := cfg.RootCAs            // nil ⇒ system trust
		cfg.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return fmt.Errorf("tls: server presented no certificate")
			}
			intermediates := x509.NewCertPool()
			for _, c := range cs.PeerCertificates[1:] {
				intermediates.AddCert(c)
			}
			_, err := cs.PeerCertificates[0].Verify(x509.VerifyOptions{
				Roots:         roots,
				Intermediates: intermediates,
				// DNSName intentionally omitted — that's the hostname check we're skipping.
			})
			return err
		}
	}

	return cfg, nil
}
