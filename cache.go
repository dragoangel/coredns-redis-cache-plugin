package redis_cache

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"time"

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
	return cacheKey(m.Question[0].Name, m.Question[0].Qtype, do)
}

var (
	one  = []byte("1")
	zero = []byte("0")
)

// cacheKey computes an FNV-32 hash key from qname, qtype, and DNSSEC DO flag.
func cacheKey(qname string, qtype uint16, do bool) string {
	h := fnv.New32()

	if do {
		h.Write(one)
	} else {
		h.Write(zero)
	}

	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, qtype)
	h.Write(b)

	for i := range qname {
		c := qname[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		h.Write([]byte{c})
	}

	return fmt.Sprintf("%d", h.Sum32())
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
			cacheDrops.WithLabelValues(w.server).Inc()
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
		if err := w.Redis.Add(w.ctx, key, m, duration); err != nil {
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
