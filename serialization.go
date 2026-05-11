package redis_cache

import (
	"github.com/miekg/dns"
)

// ToBytes packs the DNS message into wire format. The packed bytes are stored
// in Redis verbatim — RESP and go-redis are binary-safe so there's no need to
// base64-encode (which would inflate every cached entry by ~33%).
func ToBytes(m *dns.Msg) ([]byte, error) {
	return m.Pack()
}

// FromBytes unpacks a wire-format DNS message and applies the given TTL to all
// records. A wire-format error is returned to the caller so a corrupted Redis
// value is treated as a read error rather than served as an empty (NODATA-
// spoofing) reply.
func FromBytes(b []byte, ttl uint32) (*dns.Msg, error) {
	m := new(dns.Msg)
	if err := m.Unpack(b); err != nil {
		return nil, err
	}
	setMsgTTL(m, ttl)
	return m, nil
}
