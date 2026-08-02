package redis_cache

import (
	"bytes"

	"github.com/miekg/dns"
)

// cachePayloadPrefix marks the Redis payload format version. It lets us
// distinguish new cache records from legacy raw DNS wire bytes and add small
// metadata fields without breaking old entries.
// The prefix is 4 bytes long to avoid collisions with the 2-byte DNS header
// RDC2 is a mnemonic for Redis Cache Data v2.
var cachePayloadPrefix = []byte{'R', 'C', 'D', '2'}

const cachePayloadFlagDO byte = 1 << iota

// ToBytes packs the DNS message into wire format. The packed bytes are stored
// in Redis as DNS payload only: OPT is stripped because EDNS metadata is
// hop-by-hop and must not be replayed across cache hits. RESP and go-redis are
// binary-safe so there's no need to base64-encode (which would inflate every
// cached entry by ~33%).
func ToBytes(m *dns.Msg) ([]byte, error) {
	wire, err := msgWithoutOPT(m).Pack()
	if err != nil {
		return nil, err
	}

	b := make([]byte, 0, len(cachePayloadPrefix)+1+len(wire))
	b = append(b, cachePayloadPrefix...)
	var flags byte
	if opt := m.IsEdns0(); opt != nil && opt.Do() {
		flags |= cachePayloadFlagDO
	}
	b = append(b, flags)
	b = append(b, wire...)
	return b, nil
}

func msgWithoutOPT(m *dns.Msg) *dns.Msg {
	clone := m.Copy()
	if len(clone.Extra) == 0 {
		return clone
	}

	extra := clone.Extra[:0]
	for _, rr := range clone.Extra {
		if rr.Header().Rrtype != dns.TypeOPT {
			extra = append(extra, rr)
		}
	}
	clone.Extra = extra
	return clone
}

// FromBytes unpacks a wire-format DNS message and applies the given TTL to all
// records. A wire-format error is returned to the caller so a corrupted Redis
// value is treated as a read error rather than served as an empty (NODATA-
// spoofing) reply.
func FromBytes(b []byte, ttl uint32) (*dns.Msg, bool, error) {
	hasMetaPrefix := bytes.HasPrefix(b, cachePayloadPrefix)
	storedDO := false
	if hasMetaPrefix {
		if len(b) == len(cachePayloadPrefix) {
			return nil, false, dns.ErrBuf
		}
		storedDO = b[len(cachePayloadPrefix)]&cachePayloadFlagDO != 0
		b = b[len(cachePayloadPrefix)+1:]
	}

	m := new(dns.Msg)
	if err := m.Unpack(b); err != nil {
		return nil, false, err
	}
	setMsgTTL(m, ttl)
	if !hasMetaPrefix {
		if opt := m.IsEdns0(); opt != nil {
			storedDO = opt.Do()
		}
	}
	return m, storedDO, nil
}
