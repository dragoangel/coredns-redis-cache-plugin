package redis_cache

import (
	"strings"
	"testing"

	"github.com/coredns/coredns/plugin/pkg/response"
	"github.com/miekg/dns"
)

func TestCacheKey_Deterministic(t *testing.T) {
	k1 := cacheKey("example.com.", dns.ClassINET, dns.TypeA, false)
	k2 := cacheKey("example.com.", dns.ClassINET, dns.TypeA, false)
	if k1 != k2 {
		t.Fatalf("cacheKey not deterministic: %q vs %q", k1, k2)
	}
}

func TestCacheKey_Length(t *testing.T) {
	// xxhash64 → 16 hex characters.
	k := cacheKey("example.com.", dns.ClassINET, dns.TypeA, false)
	if len(k) != 16 {
		t.Fatalf("expected 16-char hex key, got %d (%q)", len(k), k)
	}
}

func TestCacheKey_CaseInsensitive(t *testing.T) {
	lower := cacheKey("example.com.", dns.ClassINET, dns.TypeA, false)
	upper := cacheKey("EXAMPLE.COM.", dns.ClassINET, dns.TypeA, false)
	mixed := cacheKey("ExAmPlE.CoM.", dns.ClassINET, dns.TypeA, false)
	if lower != upper || lower != mixed {
		t.Fatalf("case-folding broken: lower=%s upper=%s mixed=%s", lower, upper, mixed)
	}
}

func TestCacheKey_DistinguishesQClass(t *testing.T) {
	in := cacheKey("version.bind.", dns.ClassINET, dns.TypeTXT, false)
	ch := cacheKey("version.bind.", dns.ClassCHAOS, dns.TypeTXT, false)
	if in == ch {
		t.Fatalf("IN and CH must not share a key (%s)", in)
	}
}

func TestCacheKey_DistinguishesQType(t *testing.T) {
	a := cacheKey("example.com.", dns.ClassINET, dns.TypeA, false)
	aaaa := cacheKey("example.com.", dns.ClassINET, dns.TypeAAAA, false)
	if a == aaaa {
		t.Fatalf("A and AAAA must not share a key")
	}
}

func TestCacheKey_DistinguishesDO(t *testing.T) {
	off := cacheKey("example.com.", dns.ClassINET, dns.TypeA, false)
	on := cacheKey("example.com.", dns.ClassINET, dns.TypeA, true)
	if off == on {
		t.Fatalf("DO=0 and DO=1 must not share a key")
	}
}

func TestCacheKey_DistinguishesQName(t *testing.T) {
	a := cacheKey("a.example.com.", dns.ClassINET, dns.TypeA, false)
	b := cacheKey("b.example.com.", dns.ClassINET, dns.TypeA, false)
	if a == b {
		t.Fatalf("different qnames must produce different keys")
	}
}

func TestCacheKey_LongName(t *testing.T) {
	// A 250-byte qname is valid DNS; the implementation must not panic and
	// must still produce the fixed 16-char hex output.
	long := strings.Repeat("a", 248) + "."
	k := cacheKey(long, dns.ClassINET, dns.TypeA, false)
	if len(k) != 16 {
		t.Fatalf("long qname produced bad key len=%d", len(k))
	}
}

func msg(qname string, qtype uint16) *dns.Msg {
	m := new(dns.Msg)
	m.SetQuestion(qname, qtype)
	m.Response = true
	return m
}

func TestKey_SkipsTruncated(t *testing.T) {
	m := msg("example.com.", dns.TypeA)
	m.Truncated = true
	if got := key(m, response.NoError, false); got != "" {
		t.Fatalf("truncated reply must not be cached, got key %q", got)
	}
}

func TestKey_SkipsErrorMetaUpdate(t *testing.T) {
	m := msg("example.com.", dns.TypeA)
	for _, mt := range []response.Type{response.OtherError, response.Meta, response.Update} {
		if got := key(m, mt, false); got != "" {
			t.Fatalf("response.Type %v must not be cached, got key %q", mt, got)
		}
	}
}

func TestKey_SkipsZeroQuestions(t *testing.T) {
	// QDCOUNT==0 is technically allowed by the wire format but undefined by
	// the protocol; refuse to cache rather than panic on m.Question[0].
	m := new(dns.Msg)
	m.Response = true
	if got := key(m, response.NoError, false); got != "" {
		t.Fatalf("0-question reply must not be cached, got key %q", got)
	}
}

func TestKey_SkipsMultipleQuestions(t *testing.T) {
	// Multi-question DNS was never standardized; refusing to cache keeps
	// the write path symmetric with state.Match's len==1 invariant on the
	// read path, and avoids producing entries that could never be served.
	m := msg("first.example.com.", dns.TypeA)
	m.Question = append(m.Question, dns.Question{
		Name:   "second.example.com.",
		Qtype:  dns.TypeAAAA,
		Qclass: dns.ClassINET,
	})
	if got := key(m, response.NoError, false); got != "" {
		t.Fatalf("multi-question reply must not be cached, got key %q", got)
	}
}
