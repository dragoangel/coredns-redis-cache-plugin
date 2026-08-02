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
	k1 := keyer{}.key("example.com.", dns.ClassINET, dns.TypeA, false, false)
	k2 := keyer{}.key("example.com.", dns.ClassINET, dns.TypeA, false, false)
	if k1 != k2 {
		t.Fatalf("key not deterministic: %q vs %q", k1, k2)
	}
}

func TestCacheKey_Length(t *testing.T) {
	// xxhash64 → 16 hex characters.
	k := keyer{}.key("example.com.", dns.ClassINET, dns.TypeA, false, false)
	if len(k) != 16 {
		t.Fatalf("expected 16-char hex key, got %d (%q)", len(k), k)
	}
}

func TestCacheKey_CaseInsensitive(t *testing.T) {
	lower := keyer{}.key("example.com.", dns.ClassINET, dns.TypeA, false, false)
	upper := keyer{}.key("EXAMPLE.COM.", dns.ClassINET, dns.TypeA, false, false)
	mixed := keyer{}.key("ExAmPlE.CoM.", dns.ClassINET, dns.TypeA, false, false)
	if lower != upper || lower != mixed {
		t.Fatalf("case-folding broken: lower=%s upper=%s mixed=%s", lower, upper, mixed)
	}
}

func TestCacheKey_DistinguishesQClass(t *testing.T) {
	in := keyer{}.key("version.bind.", dns.ClassINET, dns.TypeTXT, false, false)
	ch := keyer{}.key("version.bind.", dns.ClassCHAOS, dns.TypeTXT, false, false)
	if in == ch {
		t.Fatalf("IN and CH must not share a key (%s)", in)
	}
}

func TestCacheKey_DistinguishesQType(t *testing.T) {
	a := keyer{}.key("example.com.", dns.ClassINET, dns.TypeA, false, false)
	aaaa := keyer{}.key("example.com.", dns.ClassINET, dns.TypeAAAA, false, false)
	if a == aaaa {
		t.Fatalf("A and AAAA must not share a key")
	}
}

func TestCacheKey_DistinguishesDO(t *testing.T) {
	off := keyer{}.key("example.com.", dns.ClassINET, dns.TypeA, false, false)
	on := keyer{}.key("example.com.", dns.ClassINET, dns.TypeA, true, false)
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
	off := keyer{}.key("example.com.", dns.ClassINET, dns.TypeA, false, false)
	on := keyer{}.key("example.com.", dns.ClassINET, dns.TypeA, false, true)
	if off == on {
		t.Fatalf("CD=0 and CD=1 must not share a key")
	}
}

func TestCacheKey_DistinguishesQName(t *testing.T) {
	a := keyer{}.key("a.example.com.", dns.ClassINET, dns.TypeA, false, false)
	b := keyer{}.key("b.example.com.", dns.ClassINET, dns.TypeA, false, false)
	if a == b {
		t.Fatalf("different qnames must produce different keys")
	}
}

func TestCacheKey_PrefixApplied(t *testing.T) {
	bare := keyer{}.key("example.com.", dns.ClassINET, dns.TypeA, false, false)
	pref := keyer{prefix: "cdrc"}.key("example.com.", dns.ClassINET, dns.TypeA, false, false)
	if pref != "cdrc:"+bare {
		t.Fatalf("expected %q, got %q", "cdrc:"+bare, pref)
	}
}

func TestCacheKey_EmptyPrefixOmitsColon(t *testing.T) {
	k := keyer{}.key("example.com.", dns.ClassINET, dns.TypeA, false, false)
	if len(k) != 16 || k[:1] == ":" {
		t.Fatalf("empty prefix must yield bare 16-hex with no separator, got %q", k)
	}
}

func TestCacheKey_LongName(t *testing.T) {
	// A 250-byte qname is valid DNS; the implementation must not panic and
	// must still produce the fixed 16-char hex output.
	long := strings.Repeat("a", 248) + "."
	k := keyer{}.key(long, dns.ClassINET, dns.TypeA, false, false)
	if len(k) != 16 {
		t.Fatalf("long qname produced bad key len=%d", len(k))
	}
}

func TestCacheKey_SeedChangesKey(t *testing.T) {
	// A non-zero hash seed must shift the key space, and seed 0 must reproduce
	// the unseeded key (xxhash.New == NewWithSeed(0)) so leaving the directive
	// unset changes nothing for existing deployments.
	unseeded := keyer{}.key("example.com.", dns.ClassINET, dns.TypeA, false, false)
	seed0 := keyer{hashSeed: 0}.key("example.com.", dns.ClassINET, dns.TypeA, false, false)
	seeded := keyer{hashSeed: 0x9e3779b97f4a7c15}.key("example.com.", dns.ClassINET, dns.TypeA, false, false)

	if seed0 != unseeded {
		t.Fatalf("seed 0 must equal the unseeded key: %q vs %q", seed0, unseeded)
	}
	if seeded == unseeded {
		t.Fatalf("a non-zero seed must change the key, both were %q", seeded)
	}
}

func msg(qname string, qtype uint16) *dns.Msg {
	m := new(dns.Msg)
	m.SetQuestion(qname, qtype)
	m.Response = true
	return m
}

func TestCacheable_SkipsTruncated(t *testing.T) {
	m := msg("example.com.", dns.TypeA)
	m.Truncated = true
	if cacheable(m, response.NoError) {
		t.Fatal("truncated reply must not be cacheable")
	}
}

func TestCacheable_SkipsErrorMetaUpdate(t *testing.T) {
	m := msg("example.com.", dns.TypeA)
	for _, mt := range []response.Type{response.OtherError, response.Meta, response.Update} {
		if cacheable(m, mt) {
			t.Fatalf("response.Type %v must not be cacheable", mt)
		}
	}
}

func TestCacheable_SkipsZeroQuestions(t *testing.T) {
	// QDCOUNT==0 is technically allowed by the wire format but undefined by
	// the protocol; refuse to cache rather than risk indexing m.Question[0].
	m := new(dns.Msg)
	m.Response = true
	if cacheable(m, response.NoError) {
		t.Fatal("0-question reply must not be cacheable")
	}
}

func TestCacheable_SkipsMultipleQuestions(t *testing.T) {
	// Multi-question DNS was never standardized; refusing to cache keeps
	// the write path symmetric with state.Match's len==1 invariant on the
	// read path, and avoids producing entries that could never be served.
	m := msg("first.example.com.", dns.TypeA)
	m.Question = append(m.Question, dns.Question{
		Name:   "second.example.com.",
		Qtype:  dns.TypeAAAA,
		Qclass: dns.ClassINET,
	})
	if cacheable(m, response.NoError) {
		t.Fatal("multi-question reply must not be cacheable")
	}
}

func TestCacheable_SkipsSOALessNameError(t *testing.T) {
	m := msg("missing.example.com.", dns.TypeA)
	m.Rcode = dns.RcodeNameError
	if cacheable(m, response.NameError) {
		t.Fatal("SOA-less NXDOMAIN reply must not be cacheable")
	}
}

func TestCacheable_CachesNameErrorWithSOA(t *testing.T) {
	m := msg("missing.example.com.", dns.TypeA)
	m.Rcode = dns.RcodeNameError
	soa, _ := dns.NewRR("example.com. 60 IN SOA ns.example.com. hostmaster.example.com. 1 3600 600 86400 60")
	m.Ns = []dns.RR{soa}
	if !cacheable(m, response.NameError) {
		t.Fatal("NXDOMAIN reply with SOA should remain cacheable")
	}
}

func TestCacheable_SkipsSOALessNODATADanglingCNAMEChain(t *testing.T) {
	m := msg("alias.example.org.", dns.TypeA)
	cname1, _ := dns.NewRR("alias.example.org. 3600 IN CNAME target1.example.net.")
	cname2, _ := dns.NewRR("target1.example.net. 3600 IN CNAME target2.example.net.")
	m.Answer = []dns.RR{cname1, cname2}
	if cacheable(m, response.NoError) {
		t.Fatal("SOA-less effective NODATA reply must not be cacheable")
	}
}

func TestCacheable_SkipsSOALessNODATAWithUnrelatedTerminalType(t *testing.T) {
	m := msg("alias.example.org.", dns.TypeA)
	cname, _ := dns.NewRR("alias.example.org. 3600 IN CNAME target.example.net.")
	aaaa, _ := dns.NewRR("target.example.net. 3600 IN AAAA ::1")
	m.Answer = []dns.RR{cname, aaaa}
	if cacheable(m, response.NoError) {
		t.Fatal("CNAME chain ending in a different type must not be cacheable without SOA")
	}
}

func TestCacheable_CachesTerminalAnswerOnCNAMEChain(t *testing.T) {
	m := msg("alias.example.org.", dns.TypeA)
	a, _ := dns.NewRR("target.example.net. 3600 IN A 127.0.0.1")
	cname, _ := dns.NewRR("alias.example.org. 3600 IN CNAME target.example.net.")
	m.Answer = []dns.RR{a, cname}
	if !cacheable(m, response.NoError) {
		t.Fatal("CNAME chain terminating in the queried type should remain cacheable")
	}
}

func TestCacheable_CachesDirectCNAMEQuery(t *testing.T) {
	m := msg("alias.example.org.", dns.TypeCNAME)
	cname, _ := dns.NewRR("alias.example.org. 3600 IN CNAME target.example.net.")
	m.Answer = []dns.RR{cname}
	if !cacheable(m, response.NoError) {
		t.Fatal("direct CNAME query answered by a CNAME should remain cacheable")
	}
}

func TestCacheable_CachesANYWithLoneCNAME(t *testing.T) {
	m := msg("alias.example.org.", dns.TypeANY)
	cname, _ := dns.NewRR("alias.example.org. 3600 IN CNAME target.example.net.")
	m.Answer = []dns.RR{cname}
	if !cacheable(m, response.NoError) {
		t.Fatal("ANY query with an answer should remain cacheable")
	}
}

func TestCacheable_SkipsMalformedCNAMEChain(t *testing.T) {
	m := msg("alias.example.org.", dns.TypeA)
	cname1, _ := dns.NewRR("alias.example.org. 3600 IN CNAME target1.example.net.")
	cname2, _ := dns.NewRR("alias.example.org. 3600 IN CNAME target2.example.net.")
	a, _ := dns.NewRR("target1.example.net. 3600 IN A 192.0.2.1")
	m.Answer = []dns.RR{cname1, cname2, a}
	if cacheable(m, response.NoError) {
		t.Fatal("malformed CNAME chain must not be cacheable without SOA")
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

// TestWriteMsg_RefusesCDMismatch pins the write-side consistency guard: the
// cache key is built from the request (CD=0 here), but if the reply carries
// CD=1 its flags disagree with the request tuple the key encodes. Caching it
// would wedge a CD=1-semantics answer into the CD=0 slot — the DNSSEC poisoning
// vector the CD split defends against — so WriteMsg must refuse it and count a
// response mismatch instead.
func TestWriteMsg_RefusesCDMismatch(t *testing.T) {
	w, _, cleanup := writeMsgFixture(t, "example.com.", dns.TypeA, time.Minute, 0, time.Minute, 0)
	defer cleanup()

	// Request has CD=0 (SetQuestion leaves it unset in the fixture).
	res := new(dns.Msg)
	res.SetReply(w.state.Req)
	res.CheckingDisabled = true // reply disagrees with the request's CD bit
	rr, _ := dns.NewRR("example.com. 60 IN A 192.0.2.1")
	res.Answer = []dns.RR{rr}

	before := testutil.ToFloat64(cacheResponseMismatches.WithLabelValues(w.server))
	if err := w.WriteMsg(res); err != nil {
		t.Fatalf("WriteMsg: %v", err)
	}
	after := testutil.ToFloat64(cacheResponseMismatches.WithLabelValues(w.server))
	if after-before != 1 {
		t.Errorf("cacheResponseMismatches: got delta %v, want 1 (CD mismatch must be refused)", after-before)
	}

	// The entry must not exist in Redis. The SET is fire-and-forget, so read
	// back through the plugin's own Get with the request-derived key: a refused
	// write means a clean miss (nil, nil), never a stored value.
	k := w.keyer.key(w.state.Name(), w.state.QClass(), w.state.QType(), w.state.Do(), w.state.Req.CheckingDisabled)
	if m, _, err := w.Get(context.Background(), k); err != nil || m != nil {
		t.Fatalf("expected clean miss for refused entry, got msg=%v err=%v", m, err)
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
