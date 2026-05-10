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
	// Defense in depth: standard DNS carries exactly one question (QDCOUNT
	// can technically be 0 or 2+ per the wire format, but the protocol
	// never defined semantics for those). Refuse to cache anything else,
	// matching state.Match's len==1 invariant on the read path so we don't
	// produce entries that could never legitimately be served.
	if len(m.Question) != 1 {
		return ""
	}
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
	// ctx breaks Go's "don't store contexts in structs" idiom on purpose:
	// dns.ResponseWriter.WriteMsg has no ctx parameter, but we need one for
	// the async Redis SET fired from WriteMsg. ServeDNS stashes the request
	// ctx here on the way in; WriteMsg derives a detached child for the SET.
	ctx context.Context
}

// WriteMsg implements the dns.ResponseWriter interface.
//
// The Redis SET runs in a fire-and-forget goroutine with a context detached
// from the request, so:
//
//   - the DNS reply to the client is never blocked on Redis latency, even when
//     Redis stalls up to writeTimeout × (maxRetries+1);
//   - a cache entry still lands when the upstream client cancels mid-flight,
//     so the next requester gets a hit instead of re-burdening upstream.
//
// Wire bytes are packed synchronously: encoding failures are observed at the
// right time, and the goroutine never reads a *dns.Msg the caller may still
// mutate after WriteMsg returns. go-redis's own writeTimeout / MaxRetries
// bound how long the goroutine can run; no extra cap is layered on top.
func (w *ResponseWriter) WriteMsg(res *dns.Msg) error {
	do := false
	mt, opt := response.Typify(res, time.Now().UTC())
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

	// Snapshot wire bytes before TTL clamping mutates `res` for the client,
	// and before WriteMsg hands the message off. Cache reads recompute TTL
	// from Redis PTTL, so the stored TTL value is throwaway anyway.
	var wire []byte
	if k != "" && duration > 0 {
		switch {
		case !w.state.Match(res):
			cacheResponseMismatches.WithLabelValues(w.server).Inc()
		case mt == response.NoError || mt == response.Delegation || mt == response.NameError || mt == response.NoData:
			b, err := ToBytes(res)
			if err != nil {
				log.Debugf("Failed to serialize DNS message for cache: %s", err)
				cacheEncodeErrors.WithLabelValues(w.server).Inc()
			} else {
				wire = b
			}
		case mt == response.OtherError:
			// don't cache
		default:
			log.Warningf("Redis called with unknown typification: %d", mt)
		}
	}

	// Apply capped TTL to this reply to avoid jarring TTL jumps.
	setMsgTTL(res, int(duration.Seconds()))
	err := w.ResponseWriter.WriteMsg(res)

	if wire != nil {
		go func() {
			// Single-flight the SET so concurrent goroutines writing the same
			// key collapse to a single Redis round-trip. Already a goroutine
			// per call, so waiters here don't block the DNS hot path.
			_, _, _ = w.writeFlight.Do(k, func() (interface{}, error) {
				if addErr := w.Add(context.WithoutCancel(w.ctx), k, wire, duration); addErr != nil {
					log.Debugf("Failed to add response to Redis cache: %s", addErr)
					cacheSetErrors.WithLabelValues(w.server).Inc()
				}
				return nil, nil
			})
		}()
	}

	return err
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

	// Plugin defaults for Redis timeouts. Read + pool wait are kept tight
	// so a single Redis miss can't stretch a DNS reply past ~1s.
	defaultConnectTimeout = 1 * time.Second
	defaultReadTimeout    = 500 * time.Millisecond
	defaultWriteTimeout   = 2 * time.Second
	defaultPoolTimeout    = 500 * time.Millisecond

	// Success is the directive for caching positive responses: success <max_ttl> [<min_ttl>]
	Success = "success"
	// Denial is the directive for caching negative responses: denial <max_ttl> [<min_ttl>]
	Denial = "denial"
)
