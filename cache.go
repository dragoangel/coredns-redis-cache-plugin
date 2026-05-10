// Package redis_cache implements a CoreDNS plugin that uses a Redis-compatible
// backend as a shared L2 DNS cache, sitting behind the in-process L1 cache.
package redis_cache

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/coredns/coredns/plugin/pkg/response"
	"github.com/coredns/coredns/request"

	"github.com/miekg/dns"
)

// key returns the cache key for a DNS message. Returns "" if the message should not be cached.
// We do not cache truncated responses, errors, meta, or update messages.
func key(m *dns.Msg, t response.Type, do bool) string {
	if m.Truncated {
		return ""
	}
	if t == response.OtherError || t == response.Meta || t == response.Update {
		return ""
	}
	return cacheKey(m.Question[0].Name, m.Question[0].Qclass, m.Question[0].Qtype, do)
}

// cacheKey returns the Redis key for a DNS question, mixing qclass, qtype,
// the DO flag, and the lowercased qname.
//
// Hash choice (compared at ~1M cached entries, the L2's stated sweet spot):
//
//	algo        key size       P(≥1 collision)
//	FNV-32       8 hex / 4 B    ~100% (50% at ~77 K)
//	xxhash64    16 hex / 8 B    ~3e-8 (50% at ~5.1 B)
//	xxhash128   32 hex / 16 B   ~3e-27 (effectively zero)
//
// xxhash64 is chosen: collisions are statistically irrelevant at any plausible
// fleet scale, and the post-fetch question check in re.get catches residual
// collisions, corruption, or version-skew, accounting them via cacheCollisions
// — so a 128-bit hash buys nothing operationally and FNV-32 is dangerous (the
// 32-bit space is birthday-bound past ~77 K entries, exploitable as a cross-
// domain substitution oracle on a shared L2).
func cacheKey(qname string, qclass, qtype uint16, do bool) string {
	var hdr [5]byte
	binary.BigEndian.PutUint16(hdr[0:2], qclass)
	binary.BigEndian.PutUint16(hdr[2:4], qtype)
	if do {
		hdr[4] = 1
	}

	h := xxhash.New()
	_, _ = h.Write(hdr[:])

	// Lowercase qname into a stack buffer to feed the hash in one Write.
	// DNS names are bounded by 255 bytes (RFC 1035), so 256 is enough.
	var buf [256]byte
	n := len(qname)
	if n > len(buf) {
		n = len(buf)
	}
	for i := 0; i < n; i++ {
		c := qname[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		buf[i] = c
	}
	_, _ = h.Write(buf[:n])

	var sum [8]byte
	binary.BigEndian.PutUint64(sum[:], h.Sum64())
	return hex.EncodeToString(sum[:])
}

// ResponseWriter is a response writer that caches the reply message in Redis.
type ResponseWriter struct {
	dns.ResponseWriter
	state request.Request
	*Redis
	server string
	ctx    context.Context
}

// WriteMsg implements the dns.ResponseWriter interface.
func (w *ResponseWriter) WriteMsg(res *dns.Msg) error {
	do := false
	mt, opt := response.Typify(res, w.now().UTC())
	if opt != nil {
		do = opt.Do()
	}

	k := key(res, mt, do)

	maxDur := w.pMaxTTL
	minDur := w.pMinTTL
	if mt == response.NameError || mt == response.NoData {
		maxDur = w.nMaxTTL
		minDur = w.nMinTTL
	}

	// Clamp: max(minDur, min(msgTTL, maxDur))
	duration := maxDur
	msgTTL := minMsgTTL(res, mt)
	if msgTTL < duration {
		duration = msgTTL
	}
	if duration < minDur {
		duration = minDur
	}

	if k != "" && duration > 0 {
		if w.state.Match(res) {
			w.set(res, k, mt, duration)
		} else {
			cacheResponseMismatches.WithLabelValues(w.server).Inc()
		}
	}

	// Apply capped TTL to this reply to avoid jarring TTL jumps.
	ttl := uint32(duration.Seconds())
	for i := range res.Answer {
		res.Answer[i].Header().Ttl = ttl
	}
	for i := range res.Ns {
		res.Ns[i].Header().Ttl = ttl
	}
	for i := range res.Extra {
		if res.Extra[i].Header().Rrtype != dns.TypeOPT {
			res.Extra[i].Header().Ttl = ttl
		}
	}
	return w.ResponseWriter.WriteMsg(res)
}

func (w *ResponseWriter) set(m *dns.Msg, key string, mt response.Type, duration time.Duration) {
	if key == "" || duration == 0 {
		return
	}

	switch mt {
	case response.NoError, response.Delegation, response.NameError, response.NoData:
		// Serialize before touching Redis so a malformed message can't poison
		// the key with an empty value, and so encoding failures are accounted
		// to encode_errors_total rather than set_errors_total (which is for
		// Redis-side write failures).
		wire, err := ToBytes(m)
		if err != nil {
			log.Debugf("Failed to serialize DNS message for cache: %s", err)
			cacheEncodeErrors.WithLabelValues(w.server).Inc()
			return
		}
		if err := w.Add(w.ctx, key, wire, duration); err != nil {
			log.Debugf("Failed to add response to Redis cache: %s", err)
			redisErr.WithLabelValues(w.server).Inc()
		}
	case response.OtherError:
		// don't cache these
	default:
		log.Warningf("Redis called with unknown typification: %d", mt)
	}
}

// Write implements the dns.ResponseWriter interface.
func (w *ResponseWriter) Write(buf []byte) (int, error) {
	log.Warningf("Redis called with Write: not caching reply")
	return w.ResponseWriter.Write(buf)
}

const (
	maxTTL      = 1 * time.Hour
	maxNTTL     = 30 * time.Minute
	failSafeTTL = 5 * time.Second

	// Redis timeouts — 3 retries, worst case: connect 3s, read 3s, write 6s.
	defaultConnectTimeout = 1 * time.Second
	defaultReadTimeout    = 1 * time.Second
	defaultWriteTimeout   = 2 * time.Second

	// Success is the directive for caching positive responses: success <max_ttl> [<min_ttl>]
	Success = "success"
	// Denial is the directive for caching negative responses: denial <max_ttl> [<min_ttl>]
	Denial = "denial"
)
