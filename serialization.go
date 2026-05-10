package redis_cache

import (
	"encoding/base64"

	"github.com/miekg/dns"
)

// ToString converts the DNS message m to a base64 encoded string.
func ToString(m *dns.Msg) string {
	b, _ := m.Pack()
	return base64.RawStdEncoding.EncodeToString(b)
}

// FromString converts a base64 encoded string back into a DNS message
// and applies the given TTL to all records.
func FromString(s string, ttl int) *dns.Msg {
	m := new(dns.Msg)
	b, err := base64.RawStdEncoding.DecodeString(s)
	if err != nil {
		return m
	}
	if err := m.Unpack(b); err != nil {
		return m
	}
	setMsgTTL(m, ttl)
	return m
}
