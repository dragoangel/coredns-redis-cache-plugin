// Package redis_cache implements a CoreDNS plugin that uses a Redis-compatible
// backend as a shared L2 DNS cache, sitting behind the in-process L1 cache.
package redis_cache

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/coredns/coredns/plugin/pkg/response"
	"github.com/coredns/coredns/request"

	"github.com/miekg/dns"
)

// cacheable reports whether a response may be cached at all. The decision looks
// only at the response: we never cache truncated replies, error / meta / update
// responses, or messages that don't carry exactly one question.
//
// The len==1 gate is defense in depth — standard DNS carries exactly one
// question (QDCOUNT can technically be 0 or 2+ per the wire format, but the
// protocol never defined semantics for those) — and it matches state.Match's
// len==1 invariant on the read path so we don't produce entries that could
// never legitimately be served.
func cacheable(m *dns.Msg, t response.Type) bool {
	if len(m.Question) != 1 {
		return false
	}
	if m.Truncated {
		return false
	}
	if t == response.NameError && !hasSOA(m) {
		return false
	}
	if t == response.NoError && !hasSOA(m) && isNODATA(m) {
		return false
	}
	if t == response.OtherError || t == response.Meta || t == response.Update {
		return false
	}
	return true
}

func hasSOA(m *dns.Msg) bool {
	for _, rr := range m.Ns {
		if rr.Header().Rrtype == dns.TypeSOA {
			return true
		}
	}
	return false
}

// isNODATA reports whether a NOERROR response with a non-empty answer section
// still fails to answer the question at the terminal owner name.
func isNODATA(m *dns.Msg) bool {
	if len(m.Answer) == 0 {
		return false
	}

	qtype := m.Question[0].Qtype
	if qtype == dns.TypeANY {
		return false
	}

	name := m.Question[0].Name
	if qtype != dns.TypeCNAME {
		terminal, ok := canonicalName(m.Answer, name)
		if !ok {
			return true
		}
		name = terminal
	}

	for _, rr := range m.Answer {
		h := rr.Header()
		if h.Rrtype == qtype && strings.EqualFold(h.Name, name) {
			return false
		}
	}

	return true
}

func canonicalName(answer []dns.RR, name string) (string, bool) {
	visited := nameSet{}
	for {
		if visited.contains(name) {
			return name, false
		}
		visited.add(name)

		target, ok := uniqueCNAMETarget(answer, name)
		if !ok {
			return name, false
		}
		if target == "" {
			return name, true
		}
		name = target
	}
}

func uniqueCNAMETarget(answer []dns.RR, owner string) (target string, ok bool) {
	for _, rr := range answer {
		cname, isCNAME := rr.(*dns.CNAME)
		if !isCNAME || !strings.EqualFold(cname.Header().Name, owner) {
			continue
		}
		if target != "" && !strings.EqualFold(target, cname.Target) {
			return "", false
		}
		target = cname.Target
	}
	return target, true
}

type nameSet map[string]struct{}

func (s nameSet) contains(name string) bool {
	_, ok := s[strings.ToLower(name)]
	return ok
}

func (s nameSet) add(name string) {
	s[strings.ToLower(name)] = struct{}{}
}

// keyer derives namespaced Redis cache keys from a DNS question tuple. It holds
// the immutable key-derivation settings so they are read from config once at
// setup and never threaded through individual calls;
type keyer struct {
	// prefix isolates this plugin's keys from anything else sharing the Redis
	// namespace. Default "cdrc"; key() formats keys as "<prefix>:<hex>". An
	// explicit empty string disables the prefix (and the colon) for operators
	// who manage isolation out-of-band.
	prefix string
	// hashSeed seeds the xxhash. 0 (go's library default via xxhash.New) is an
	// unseeded hash and reproduces the historical keys, so leaving it unset
	// changes nothing. A non-zero seed — which must be identical across every
	// instance sharing the same Redis — makes the key space unpredictable to an
	// attacker who would otherwise construct qnames that collide with a chosen
	// victim key offline (xxhash is not collision-resistant). Changing it
	// invalidates every existing entry, since all keys shift.
	hashSeed uint64
}

// key returns the Redis key for a DNS question, mixing qclass, qtype, the DO
// flag, the CD flag, and the lowercased qname.
//
// CD (Checking Disabled, RFC 4035 §3.2.2) is in the key, not just verified
// after fetch, because mixing CD=0 and CD=1 entries under one key is a real
// poisoning vector against DNSSEC: a validating upstream returns SERVFAIL for
// a bogus name when CD=0, but returns the unvalidated record when CD=1. If
// both shared a cache slot, a CD=1 query (any attacker) would overwrite the
// SERVFAIL entry with the bogus record and CD=0 clients — who trust DNSSEC —
// would receive forged data without SERVFAIL. Splitting by CD removes the
// shared slot; the post-fetch verify in re.get is the belt over the braces.
//
// Hash choice (compared at ~1M cached entries, the L2's stated sweet spot):
//
//	algo        key size       P(≥1 collision)
//	FNV-32       8 hex / 4 B    ~100% (50% at ~77 K)
//	xxhash64    16 hex / 8 B    ~3e-8 (50% at ~5.1 B)
//	xxhash128   32 hex / 16 B   ~3e-27 (effectively zero)
//
// xxhash64 is chosen: collisions are statistically irrelevant at any plausible
// fleet scale, and the post-fetch verify in re.get is a cheap backstop that
// turns the rare statistical collision into a counted miss rather than a
// wrong answer — so a 128-bit hash buys nothing operationally and FNV-32 is
// dangerous (the 32-bit space is birthday-bound past ~77 K entries,
// exploitable as a cross-domain substitution oracle on a shared L2).
func (k keyer) key(qname string, qclass, qtype uint16, do, cd bool) string {
	var hdr [6]byte
	binary.BigEndian.PutUint16(hdr[0:2], qclass)
	binary.BigEndian.PutUint16(hdr[2:4], qtype)
	if do {
		hdr[4] = 1
	}
	if cd {
		hdr[5] = 1
	}

	h := xxhash.NewWithSeed(k.hashSeed)
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
	hexsum := hex.EncodeToString(sum[:])
	if k.prefix == "" {
		return hexsum
	}
	return k.prefix + ":" + hexsum
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
	mt, opt := response.Typify(res, time.Now())
	if opt != nil {
		do = opt.Do()
	}

	// The cache key mixes the request's question (name, qtype, qclass) and CD
	// bit with the RESPONSE's DO bit.
	//
	// DO comes from the response because it labels the *content*: a DO=1 reply
	// carries DNSSEC records and belongs in the DO=1 slot, so a DO=0 client
	// (which must not be sent DNSSEC RRs, RFC 4035 §3.2.1) looks up the DO=0
	// slot and cleanly misses instead of being served RRSIGs it never asked
	// for. Because we store the very response we keyed on, a stored entry's DO
	// always equals its slot's DO, which keeps re.get's post-fetch DO check a
	// pure collision check. Cacheability, by contrast, is a property of the
	// response.
	k := ""
	if cacheable(res, mt) {
		k = w.keyer.key(w.state.Name(), w.state.QClass(), w.state.QType(), do, w.state.Req.CheckingDisabled)
	}

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
		// Only cache a reply that answers this request on the question (name,
		// qtype, qclass) and the CD bit; a mismatch means the reply is for a
		// different question (bug / spoof) or an upstream cleared CD, so refuse
		// it rather than store it under this request's key.
		//
		// DO is deliberately NOT part of this check: the key already takes DO
		// from the response (see above), so a DO=1 reply to a DO=0 request is
		// filed in the DO=1 slot instead of being rejected — the cache stays
		// effective for validating forwarders while the DO=0 lookup correctly
		// misses it.
		if !w.responseMatchesRequest(res) {
			cacheResponseMismatches.WithLabelValues(w.server).Inc()
		} else if b, err := ToBytes(res); err != nil {
			log.Debugf("Failed to serialize DNS message for cache: %s", err)
			cacheEncodeErrors.WithLabelValues(w.server).Inc()
		} else {
			wire = b
		}
	}

	// Apply capped TTL to this reply to avoid jarring TTL jumps.
	// duration is always non-negative by construction (clamp above), so the
	// uint32 cast is safe.
	setMsgTTL(res, uint32(duration.Seconds()))
	err := w.ResponseWriter.WriteMsg(res)

	if wire != nil {
		go func() {
			// recover() guards against a panic in the redis client crashing
			// the whole CoreDNS process from a fire-and-forget goroutine.
			defer func() {
				if r := recover(); r != nil {
					log.Errorf("panic in cache write for %s: %v", k, r)
				}
			}()
			// Single-flight the SET so concurrent goroutines writing the same
			// key collapse to a single Redis round-trip. Already a goroutine
			// per call, so waiters here don't block the DNS hot path.
			_, _, _ = w.writeFlight.Do(k, func() (interface{}, error) {
				if addErr := w.Add(context.WithoutCancel(w.ctx), k, wire, duration); addErr != nil {
					log.Debugf("Failed to add response to Redis cache: %s", addErr)
					cacheSetErrors.WithLabelValues(w.server, errReason(addErr)).Inc()
				}
				return nil, nil
			})
		}()
	}

	return err
}

// responseMatchesRequest reports whether res answers the pending request on the
// question (name and qtype via state.Match, plus qclass) and the CD flag — the
// request-derived components of the cache key. The DO bit is excluded because
// the key takes DO from the response, not the request (see the caller), so a
// DO=1 reply to a DO=0 request is filed under a different slot rather than
// rejected here.
//
// state.Match already guarantees len(res.Question) == 1, so res.Question[0] is
// safe to index here.
func (w *ResponseWriter) responseMatchesRequest(res *dns.Msg) bool {
	return w.state.Match(res) &&
		res.Question[0].Qclass == w.state.QClass() &&
		res.CheckingDisabled == w.state.Req.CheckingDisabled
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

	// defaultKeyPrefix isolates cache entries in a shared Redis namespace.
	// Override via the `key_prefix` directive; set to "" to disable.
	defaultKeyPrefix = "cdrc"

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
