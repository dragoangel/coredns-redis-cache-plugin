package redis_cache

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/coredns/coredns/plugin/pkg/dnstest"
	"github.com/coredns/coredns/plugin/pkg/response"
	"github.com/coredns/coredns/plugin/test"
	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestCacheKey_Deterministic(t *testing.T) {
	k1 := cacheKey("", "example.com.", dns.ClassINET, dns.TypeA, false, false)
	k2 := cacheKey("", "example.com.", dns.ClassINET, dns.TypeA, false, false)
	if k1 != k2 {
		t.Fatalf("cacheKey not deterministic: %q vs %q", k1, k2)
	}
}

func TestCacheKey_Length(t *testing.T) {
	// xxhash64 → 16 hex characters.
	k := cacheKey("", "example.com.", dns.ClassINET, dns.TypeA, false, false)
	if len(k) != 16 {
		t.Fatalf("expected 16-char hex key, got %d (%q)", len(k), k)
	}
}

func TestCacheKey_CaseInsensitive(t *testing.T) {
	lower := cacheKey("", "example.com.", dns.ClassINET, dns.TypeA, false, false)
	upper := cacheKey("", "EXAMPLE.COM.", dns.ClassINET, dns.TypeA, false, false)
	mixed := cacheKey("", "ExAmPlE.CoM.", dns.ClassINET, dns.TypeA, false, false)
	if lower != upper || lower != mixed {
		t.Fatalf("case-folding broken: lower=%s upper=%s mixed=%s", lower, upper, mixed)
	}
}

func TestCacheKey_DistinguishesQClass(t *testing.T) {
	in := cacheKey("", "version.bind.", dns.ClassINET, dns.TypeTXT, false, false)
	ch := cacheKey("", "version.bind.", dns.ClassCHAOS, dns.TypeTXT, false, false)
	if in == ch {
		t.Fatalf("IN and CH must not share a key (%s)", in)
	}
}

func TestCacheKey_DistinguishesQType(t *testing.T) {
	a := cacheKey("", "example.com.", dns.ClassINET, dns.TypeA, false, false)
	aaaa := cacheKey("", "example.com.", dns.ClassINET, dns.TypeAAAA, false, false)
	if a == aaaa {
		t.Fatalf("A and AAAA must not share a key")
	}
}

func TestCacheKey_DistinguishesDO(t *testing.T) {
	off := cacheKey("", "example.com.", dns.ClassINET, dns.TypeA, false, false)
	on := cacheKey("", "example.com.", dns.ClassINET, dns.TypeA, true, false)
	if off == on {
		t.Fatalf("DO=0 and DO=1 must not share a key")
	}
}

func TestCacheKey_DistinguishesCD(t *testing.T) {
	// CD=1 (Checking Disabled, RFC 4035 §3.2.2) tells a validating upstream to
	// return data without DNSSEC validation. For a DNSSEC-bogus name a CD=0
	// query gets SERVFAIL, a CD=1 query gets the unvalidated record. Sharing
	// one cache slot across CD values would let any CD=1 requester poison the
	// cache against CD=0 DNSSEC-trusting clients — a real bypass of DNSSEC,
	// not just a hash collision. Splitting by CD removes the shared slot.
	off := cacheKey("", "example.com.", dns.ClassINET, dns.TypeA, false, false)
	on := cacheKey("", "example.com.", dns.ClassINET, dns.TypeA, false, true)
	if off == on {
		t.Fatalf("CD=0 and CD=1 must not share a key")
	}
}

func TestCacheKey_DistinguishesQName(t *testing.T) {
	a := cacheKey("", "a.example.com.", dns.ClassINET, dns.TypeA, false, false)
	b := cacheKey("", "b.example.com.", dns.ClassINET, dns.TypeA, false, false)
	if a == b {
		t.Fatalf("different qnames must produce different keys")
	}
}

func TestCacheKey_PrefixApplied(t *testing.T) {
	bare := cacheKey("", "example.com.", dns.ClassINET, dns.TypeA, false, false)
	pref := cacheKey("cdrc", "example.com.", dns.ClassINET, dns.TypeA, false, false)
	if pref != "cdrc:"+bare {
		t.Fatalf("expected %q, got %q", "cdrc:"+bare, pref)
	}
}

func TestCacheKey_EmptyPrefixOmitsColon(t *testing.T) {
	k := cacheKey("", "example.com.", dns.ClassINET, dns.TypeA, false, false)
	if len(k) != 16 || k[:1] == ":" {
		t.Fatalf("empty prefix must yield bare 16-hex with no separator, got %q", k)
	}
}

func TestCacheKey_LongName(t *testing.T) {
	// A 250-byte qname is valid DNS; the implementation must not panic and
	// must still produce the fixed 16-char hex output.
	long := strings.Repeat("a", 248) + "."
	k := cacheKey("", long, dns.ClassINET, dns.TypeA, false, false)
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
	if got := key("", m, response.NoError, false); got != "" {
		t.Fatalf("truncated reply must not be cached, got key %q", got)
	}
}

func TestKey_SkipsErrorMetaUpdate(t *testing.T) {
	m := msg("example.com.", dns.TypeA)
	for _, mt := range []response.Type{response.OtherError, response.Meta, response.Update} {
		if got := key("", m, mt, false); got != "" {
			t.Fatalf("response.Type %v must not be cached, got key %q", mt, got)
		}
	}
}

func TestKey_SkipsZeroQuestions(t *testing.T) {
	// QDCOUNT==0 is technically allowed by the wire format but undefined by
	// the protocol; refuse to cache rather than panic on m.Question[0].
	m := new(dns.Msg)
	m.Response = true
	if got := key("", m, response.NoError, false); got != "" {
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
	if got := key("", m, response.NoError, false); got != "" {
		t.Fatalf("multi-question reply must not be cached, got key %q", got)
	}
}

// writeMsgFixture wires a *ResponseWriter against a miniredis-backed Redis
// and a dnstest.Recorder so a test can call w.WriteMsg(res) and then assert
// what landed on the downstream writer (Rcode + RR TTLs).
func writeMsgFixture(t *testing.T, qname string, qtype uint16, pMax, pMin, nMax, nMin time.Duration) (*ResponseWriter, *dnstest.Recorder, func()) {
	t.Helper()
	re, _, cleanup := newTestRedis(t)
	re.pMaxTTL = pMax
	re.pMinTTL = pMin
	re.nMaxTTL = nMax
	re.nMinTTL = nMin

	q := new(dns.Msg)
	q.SetQuestion(qname, qtype)
	state := request.Request{W: &test.ResponseWriter{}, Req: q}

	rec := dnstest.NewRecorder(&test.ResponseWriter{})
	w := &ResponseWriter{
		ResponseWriter: rec,
		Redis:          re,
		state:          state,
		ctx:            context.Background(),
	}
	return w, rec, cleanup
}

func TestWriteMsg_TTLClamp_PositiveAboveMax(t *testing.T) {
	w, rec, cleanup := writeMsgFixture(t, "example.com.", dns.TypeA, time.Minute, 0, time.Minute, 0)
	defer cleanup()

	res := new(dns.Msg)
	res.SetReply(w.state.Req)
	rr, _ := dns.NewRR("example.com. 3600 IN A 192.0.2.1")
	res.Answer = []dns.RR{rr}

	if err := w.WriteMsg(res); err != nil {
		t.Fatalf("WriteMsg: %v", err)
	}
	if got := rec.Msg.Answer[0].Header().Ttl; got != 60 {
		t.Errorf("answer TTL: got %d, want 60 (clamped to pMaxTTL)", got)
	}
}

func TestWriteMsg_TTLClamp_PositiveBelowMin(t *testing.T) {
	w, rec, cleanup := writeMsgFixture(t, "example.com.", dns.TypeA, time.Hour, 30*time.Second, time.Minute, 0)
	defer cleanup()

	res := new(dns.Msg)
	res.SetReply(w.state.Req)
	rr, _ := dns.NewRR("example.com. 5 IN A 192.0.2.1")
	res.Answer = []dns.RR{rr}

	if err := w.WriteMsg(res); err != nil {
		t.Fatalf("WriteMsg: %v", err)
	}
	if got := rec.Msg.Answer[0].Header().Ttl; got != 30 {
		t.Errorf("answer TTL: got %d, want 30 (raised to pMinTTL floor)", got)
	}
}

func TestWriteMsg_TTLClamp_PositiveWithinBounds(t *testing.T) {
	w, rec, cleanup := writeMsgFixture(t, "example.com.", dns.TypeA, time.Hour, time.Minute, time.Minute, 0)
	defer cleanup()

	res := new(dns.Msg)
	res.SetReply(w.state.Req)
	rr, _ := dns.NewRR("example.com. 120 IN A 192.0.2.1")
	res.Answer = []dns.RR{rr}

	if err := w.WriteMsg(res); err != nil {
		t.Fatalf("WriteMsg: %v", err)
	}
	if got := rec.Msg.Answer[0].Header().Ttl; got != 120 {
		t.Errorf("answer TTL: got %d, want 120 (untouched, within bounds)", got)
	}
}

func TestWriteMsg_TTLClamp_NXDOMAINAboveDenialMax(t *testing.T) {
	w, rec, cleanup := writeMsgFixture(t, "nx.example.com.", dns.TypeA, time.Hour, 0, 10*time.Minute, 0)
	defer cleanup()

	res := new(dns.Msg)
	res.SetReply(w.state.Req)
	res.Rcode = dns.RcodeNameError
	soa, _ := dns.NewRR("example.com. 86400 IN SOA ns hostmaster 1 7200 3600 604800 3600")
	res.Ns = []dns.RR{soa}

	if err := w.WriteMsg(res); err != nil {
		t.Fatalf("WriteMsg: %v", err)
	}
	// SOA.Minttl = 3600s, nMaxTTL = 600s → clamp down to 600.
	if got := rec.Msg.Ns[0].Header().Ttl; got != 600 {
		t.Errorf("authority TTL: got %d, want 600 (clamped to nMaxTTL)", got)
	}
}

func TestWriteMsg_TTLClamp_NXDOMAINBelowDenialMin(t *testing.T) {
	w, rec, cleanup := writeMsgFixture(t, "nx.example.com.", dns.TypeA, time.Hour, 0, 10*time.Minute, 30*time.Second)
	defer cleanup()

	res := new(dns.Msg)
	res.SetReply(w.state.Req)
	res.Rcode = dns.RcodeNameError
	soa, _ := dns.NewRR("example.com. 86400 IN SOA ns hostmaster 1 7200 3600 604800 5")
	res.Ns = []dns.RR{soa}

	if err := w.WriteMsg(res); err != nil {
		t.Fatalf("WriteMsg: %v", err)
	}
	// SOA.Minttl = 5s, nMinTTL = 30s → raised to 30 (denial floor).
	if got := rec.Msg.Ns[0].Header().Ttl; got != 30 {
		t.Errorf("authority TTL: got %d, want 30 (raised to nMinTTL)", got)
	}
}

func TestWriteMsg_EncodeErrorBumpsMetric(t *testing.T) {
	// A malformed reply (TXT segment > 255 bytes is a hard RFC limit that
	// miekg/dns rejects at Pack time) exercises the ToBytes-error branch
	// and must increment cacheEncodeErrors; without this test the metric
	// would be dead code.
	w, _, cleanup := writeMsgFixture(t, "example.com.", dns.TypeTXT, time.Minute, 0, time.Minute, 0)
	defer cleanup()

	res := new(dns.Msg)
	res.SetReply(w.state.Req)
	res.Answer = []dns.RR{
		&dns.TXT{
			Hdr: dns.RR_Header{Name: "example.com.", Rrtype: dns.TypeTXT, Class: dns.ClassINET, Ttl: 60},
			Txt: []string{strings.Repeat("x", 300)}, // > 255 → Pack fails
		},
	}

	before := testutil.ToFloat64(cacheEncodeErrors.WithLabelValues(w.server))
	if err := w.WriteMsg(res); err != nil {
		t.Fatalf("WriteMsg: %v", err)
	}
	after := testutil.ToFloat64(cacheEncodeErrors.WithLabelValues(w.server))
	if after-before != 1 {
		t.Errorf("cacheEncodeErrors: got delta %v, want 1", after-before)
	}
}

func TestWriteMsg_TTLClamp_DelegationUsesPositiveBounds(t *testing.T) {
	// A referral / delegation reply (NoError + empty Answer + NS records in
	// Authority) must be clamped by the SUCCESS bounds, using the NS TTL as
	// msgTTL. Previously minMsgTTL silently returned 0 for response.Delegation,
	// which collapsed duration to pMinTTL (or 0) regardless of upstream TTL.
	w, rec, cleanup := writeMsgFixture(t, "delegated.example.", dns.TypeA, time.Hour, time.Minute, time.Minute, 0)
	defer cleanup()

	res := new(dns.Msg)
	res.SetReply(w.state.Req)
	ns1, _ := dns.NewRR("delegated.example. 1800 IN NS ns1.delegated.example.")
	ns2, _ := dns.NewRR("delegated.example. 1800 IN NS ns2.delegated.example.")
	res.Ns = []dns.RR{ns1, ns2}

	if err := w.WriteMsg(res); err != nil {
		t.Fatalf("WriteMsg: %v", err)
	}
	// NS TTL = 1800s, pMaxTTL = 3600, pMinTTL = 60 → clamp = 1800 (passthrough).
	if got := rec.Msg.Ns[0].Header().Ttl; got != 1800 {
		t.Errorf("delegation TTL: got %d, want 1800 (NS TTL passthrough within bounds)", got)
	}
}

func TestWriteMsg_TTLClamp_DoesNotTouchOPT(t *testing.T) {
	w, rec, cleanup := writeMsgFixture(t, "example.com.", dns.TypeA, time.Minute, 0, time.Minute, 0)
	defer cleanup()

	res := new(dns.Msg)
	res.SetReply(w.state.Req)
	rr, _ := dns.NewRR("example.com. 3600 IN A 192.0.2.1")
	res.Answer = []dns.RR{rr}

	// OPT in Extra: the wire-level "TTL" field encodes flags + extended
	// rcode, NOT a real TTL. The clamping loop must skip it.
	opt := &dns.OPT{
		Hdr: dns.RR_Header{Name: ".", Rrtype: dns.TypeOPT, Class: 4096},
	}
	opt.SetDo()
	res.Extra = []dns.RR{opt}

	if err := w.WriteMsg(res); err != nil {
		t.Fatalf("WriteMsg: %v", err)
	}
	got := rec.Msg.Extra[0]
	if got.Header().Rrtype != dns.TypeOPT {
		t.Fatalf("expected OPT in Extra, got %T", got)
	}
	// The OPT "TTL" (= flags+extrcode) must be preserved as-is. SetDo()
	// sets the DO bit in the upper half; clamping to ~60 would wipe it.
	if optOut := got.(*dns.OPT); !optOut.Do() {
		t.Errorf("OPT DO bit lost; clamp loop must skip OPT records")
	}
}
