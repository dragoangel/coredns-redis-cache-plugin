package redis_cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/coredns/coredns/plugin/pkg/dnstest"
	"github.com/coredns/coredns/plugin/test"
	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
	"github.com/redis/go-redis/v9"
)

// newTestRedis starts a miniredis and returns a Redis plugin wired to it.
// The returned cleanup closes both.
func newTestRedis(t *testing.T) (*Redis, *miniredis.Miniredis, func()) {
	t.Helper()
	mr := miniredis.RunT(t)

	re := New()
	re.Zones = []string{"."}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	re.writeClient = client
	re.readClient = client
	cleanup := func() { _ = client.Close() }
	return re, mr, cleanup
}

func TestAddGetRoundtrip(t *testing.T) {
	re, _, cleanup := newTestRedis(t)
	defer cleanup()

	in := new(dns.Msg)
	in.SetQuestion("example.com.", dns.TypeA)
	in.Response = true
	rr, _ := dns.NewRR("example.com. 60 IN A 192.0.2.1")
	in.Answer = []dns.RR{rr}

	k := keyer{prefix: "cdrc"}.key("example.com.", dns.ClassINET, dns.TypeA, false, false)
	wire, err := ToBytes(in)
	if err != nil {
		t.Fatalf("ToBytes: %v", err)
	}
	if err := re.Add(context.Background(), k, wire, time.Minute); err != nil {
		t.Fatalf("Add: %v", err)
	}

	out, _, err := re.Get(context.Background(), k)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if out == nil {
		t.Fatal("Get returned nil for a known key")
	}
	if out.Question[0].Name != "example.com." || out.Question[0].Qtype != dns.TypeA {
		t.Fatalf("roundtripped question mismatched: %#v", out.Question)
	}
	if len(out.Answer) != 1 {
		t.Fatalf("answer lost in roundtrip: %#v", out.Answer)
	}
}

func TestGet_MultiReplicaPoolRandomLB(t *testing.T) {
	// Three replicas, same key, distinct values per replica. With random
	// load-balancing, enough Get calls will hit every replica at least once.
	// The probability of missing any one replica over N=200 picks is
	// (2/3)^200 ≈ 1e-35 — flake risk is effectively zero.
	const replicas = 3
	mrs := make([]*miniredis.Miniredis, replicas)
	addrs := make([]string, replicas)
	for i := range replicas {
		mrs[i] = miniredis.RunT(t)
		addrs[i] = mrs[i].Addr()
	}

	// Each replica holds the same DNS message but tagged with its own A record.
	const qname = "lb.example.com."
	k := keyer{prefix: "cdrc"}.key(qname, dns.ClassINET, dns.TypeA, false, false)
	wantIPs := make(map[string]bool)
	for i, mr := range mrs {
		m := new(dns.Msg)
		m.SetQuestion(qname, dns.TypeA)
		m.Response = true
		ip := fmt.Sprintf("192.0.2.%d", 10+i)
		rr, _ := dns.NewRR(qname + " 60 IN A " + ip)
		m.Answer = []dns.RR{rr}
		wire, err := ToBytes(m)
		if err != nil {
			t.Fatalf("ToBytes: %v", err)
		}
		mr.Set(k, string(wire))
		mr.SetTTL(k, time.Minute)
		wantIPs[ip] = true
	}

	re := New()
	re.Zones = []string{"."}
	clients := make([]*redis.Client, replicas)
	for i, addr := range addrs {
		clients[i] = redis.NewClient(&redis.Options{Addr: addr})
	}
	re.writeClient = clients[0] // never invoked here, but New requires non-nil for close()
	re.readPool = &readReplicaPool{clients: clients}
	defer func() { _ = re.close() }()

	got := make(map[string]int)
	for range 200 {
		m, _, err := re.Get(context.Background(), k)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if m == nil || len(m.Answer) == 0 {
			t.Fatalf("expected non-empty reply")
		}
		ip := m.Answer[0].(*dns.A).A.String()
		got[ip]++
	}
	for ip := range wantIPs {
		if got[ip] == 0 {
			t.Errorf("replica with IP %s never picked over 200 calls (distribution: %v)", ip, got)
		}
	}
}

func TestApplyClusterReadRouting(t *testing.T) {
	// Pins the cluster-mode read-routing matrix so a future cleanup of the
	// switch can't accidentally flip the `primary` branch from no-op to
	// ReadOnly=true (which would silently start routing reads to replicas).
	cases := []struct {
		readFrom       string
		wantReadOnly   bool
		wantRouteRand  bool
		wantRouteByLat bool
	}{
		{readFrom: "", wantReadOnly: true, wantRouteByLat: true}, // default = latency
		{readFrom: "latency", wantReadOnly: true, wantRouteByLat: true},
		{readFrom: "random", wantReadOnly: true, wantRouteRand: true},
		{readFrom: "primary", wantReadOnly: false}, // critical: no-op branch
	}
	for _, tc := range cases {
		t.Run(tc.readFrom, func(t *testing.T) {
			opts := &redis.ClusterOptions{}
			applyClusterReadRouting(opts, tc.readFrom)
			if opts.ReadOnly != tc.wantReadOnly {
				t.Errorf("ReadOnly: got %v, want %v", opts.ReadOnly, tc.wantReadOnly)
			}
			if opts.RouteRandomly != tc.wantRouteRand {
				t.Errorf("RouteRandomly: got %v, want %v", opts.RouteRandomly, tc.wantRouteRand)
			}
			if opts.RouteByLatency != tc.wantRouteByLat {
				t.Errorf("RouteByLatency: got %v, want %v", opts.RouteByLatency, tc.wantRouteByLat)
			}
		})
	}
}

func TestGet_Miss(t *testing.T) {
	re, _, cleanup := newTestRedis(t)
	defer cleanup()

	out, _, err := re.Get(context.Background(), "no-such-key")
	if err != nil {
		t.Fatalf("Get on missing key returned err=%v", err)
	}
	if out != nil {
		t.Fatalf("Get on missing key returned non-nil msg: %#v", out)
	}
}

func TestGet_GarbagePropagatesError(t *testing.T) {
	// A corrupt Redis value must surface as an error so the caller treats
	// it as a read error rather than an empty NODATA reply.
	re, mr, cleanup := newTestRedis(t)
	defer cleanup()

	const k = "corrupt"
	// A few bytes is shorter than the 12-byte DNS header — guaranteed unpack failure.
	mr.Set(k, "\x00\x01\x02")

	out, _, err := re.Get(context.Background(), k)
	if err == nil {
		t.Fatalf("expected decode error, got msg=%#v", out)
	}
	if out != nil {
		t.Fatalf("expected nil msg on decode error, got %#v", out)
	}
}

// fakeNext is a plugin.Handler that records that it was called and returns a
// canned response.
type fakeNext struct {
	called bool
	rcode  int
	answer []dns.RR
}

func (f *fakeNext) ServeDNS(_ context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	f.called = true
	resp := new(dns.Msg)
	resp.SetReply(r)
	resp.Rcode = f.rcode
	resp.Answer = f.answer
	_ = w.WriteMsg(resp)
	return f.rcode, nil
}
func (f *fakeNext) Name() string { return "fakeNext" }

func TestServeDNS_CacheHit(t *testing.T) {
	re, _, cleanup := newTestRedis(t)
	defer cleanup()

	// Pre-populate the cache for example.com. A.
	cached := new(dns.Msg)
	cached.SetQuestion("example.com.", dns.TypeA)
	cached.Response = true
	rr, _ := dns.NewRR("example.com. 60 IN A 192.0.2.1")
	cached.Answer = []dns.RR{rr}

	k := keyer{prefix: "cdrc"}.key("example.com.", dns.ClassINET, dns.TypeA, false, false)
	wire, err := ToBytes(cached)
	if err != nil {
		t.Fatalf("ToBytes: %v", err)
	}
	if err := re.Add(context.Background(), k, wire, time.Minute); err != nil {
		t.Fatalf("Add: %v", err)
	}

	next := &fakeNext{}
	re.Next = next

	q := new(dns.Msg)
	q.SetQuestion("example.com.", dns.TypeA)

	rec := dnstest.NewRecorder(&test.ResponseWriter{})
	rcode, err := re.ServeDNS(context.Background(), rec, q)
	if err != nil {
		t.Fatalf("ServeDNS: %v", err)
	}
	if rcode != dns.RcodeSuccess {
		t.Fatalf("rcode: got %d, want %d", rcode, dns.RcodeSuccess)
	}
	if next.called {
		t.Fatal("Next was invoked on a cache hit; it should be served from Redis")
	}
	if rec.Msg == nil || len(rec.Msg.Answer) != 1 {
		t.Fatalf("expected 1 answer, got msg=%#v", rec.Msg)
	}
}

func TestServeDNS_CacheHit_DropsCachedOPT(t *testing.T) {
	re, _, cleanup := newTestRedis(t)
	defer cleanup()

	cached := new(dns.Msg)
	cached.SetQuestion("example.com.", dns.TypeA)
	cached.Response = true
	rr, _ := dns.NewRR("example.com. 60 IN A 192.0.2.1")
	cached.Answer = []dns.RR{rr}
	opt := &dns.OPT{Hdr: dns.RR_Header{Name: ".", Rrtype: dns.TypeOPT, Class: 1232}}
	opt.SetDo()
	cached.Extra = []dns.RR{opt}

	k := keyer{prefix: "cdrc"}.key("example.com.", dns.ClassINET, dns.TypeA, true, false)
	wire, err := ToBytes(cached)
	if err != nil {
		t.Fatalf("ToBytes: %v", err)
	}
	if err := re.Add(context.Background(), k, wire, time.Minute); err != nil {
		t.Fatalf("Add: %v", err)
	}

	re.Next = &fakeNext{}
	q := new(dns.Msg)
	q.SetQuestion("example.com.", dns.TypeA)
	q.Extra = []dns.RR{opt}

	rec := dnstest.NewRecorder(&test.ResponseWriter{})
	if _, err := re.ServeDNS(context.Background(), rec, q); err != nil {
		t.Fatalf("ServeDNS: %v", err)
	}
	if rec.Msg == nil {
		t.Fatal("no message written to client")
	}
	for _, extra := range rec.Msg.Extra {
		if extra.Header().Rrtype == dns.TypeOPT {
			t.Fatalf("cache hit must not replay cached OPT, got %#v", rec.Msg.Extra)
		}
	}
}

// TestServeDNS_CacheHit_NXDOMAIN pins the negative-response hit path: a cached
// NXDOMAIN must be served (and reported) as NXDOMAIN, not silently rewritten to
// NOERROR/NODATA by SetReply forcing Rcode to RcodeSuccess.
func TestServeDNS_CacheHit_NXDOMAIN(t *testing.T) {
	re, _, cleanup := newTestRedis(t)
	defer cleanup()

	// Pre-populate the cache with an NXDOMAIN for absent.example.com. A,
	// carrying an SOA in the authority section the way a real denial does.
	cached := new(dns.Msg)
	cached.SetQuestion("absent.example.com.", dns.TypeA)
	cached.Response = true
	cached.Rcode = dns.RcodeNameError
	soa, _ := dns.NewRR("example.com. 60 IN SOA ns.example.com. hostmaster.example.com. 1 3600 600 86400 60")
	cached.Ns = []dns.RR{soa}

	k := keyer{prefix: "cdrc"}.key("absent.example.com.", dns.ClassINET, dns.TypeA, false, false)
	wire, err := ToBytes(cached)
	if err != nil {
		t.Fatalf("ToBytes: %v", err)
	}
	if err := re.Add(context.Background(), k, wire, time.Minute); err != nil {
		t.Fatalf("Add: %v", err)
	}

	next := &fakeNext{}
	re.Next = next

	q := new(dns.Msg)
	q.SetQuestion("absent.example.com.", dns.TypeA)

	rec := dnstest.NewRecorder(&test.ResponseWriter{})
	rcode, err := re.ServeDNS(context.Background(), rec, q)
	if err != nil {
		t.Fatalf("ServeDNS: %v", err)
	}
	if next.called {
		t.Fatal("Next was invoked on a cache hit; it should be served from Redis")
	}
	if rcode != dns.RcodeNameError {
		t.Fatalf("returned rcode: got %d, want %d (NXDOMAIN)", rcode, dns.RcodeNameError)
	}
	if rec.Msg == nil {
		t.Fatal("no message written to client")
	}
	if rec.Msg.Rcode != dns.RcodeNameError {
		t.Fatalf("written rcode: got %d, want %d (NXDOMAIN)", rec.Msg.Rcode, dns.RcodeNameError)
	}
}

// TestServeDNS_CacheHit_SetsAuthoritative pins that a served cache copy carries
// AA=1 even when the stored reply had it unset, matching CoreDNS's cache plugin
// (some legacy stub resolvers discard non-authoritative replies).
func TestServeDNS_CacheHit_SetsAuthoritative(t *testing.T) {
	re, _, cleanup := newTestRedis(t)
	defer cleanup()

	cached := new(dns.Msg)
	cached.SetQuestion("example.com.", dns.TypeA)
	cached.Response = true
	cached.Authoritative = false // stored copy is not authoritative
	rr, _ := dns.NewRR("example.com. 60 IN A 192.0.2.1")
	cached.Answer = []dns.RR{rr}

	k := keyer{prefix: "cdrc"}.key("example.com.", dns.ClassINET, dns.TypeA, false, false)
	wire, err := ToBytes(cached)
	if err != nil {
		t.Fatalf("ToBytes: %v", err)
	}
	if err := re.Add(context.Background(), k, wire, time.Minute); err != nil {
		t.Fatalf("Add: %v", err)
	}

	re.Next = &fakeNext{}
	q := new(dns.Msg)
	q.SetQuestion("example.com.", dns.TypeA)

	rec := dnstest.NewRecorder(&test.ResponseWriter{})
	if _, err := re.ServeDNS(context.Background(), rec, q); err != nil {
		t.Fatalf("ServeDNS: %v", err)
	}
	if rec.Msg == nil {
		t.Fatal("no message written to client")
	}
	if !rec.Msg.Authoritative {
		t.Fatal("served cache copy must have AA=1")
	}
}

func TestServeDNS_CacheHit_ClearsADForPlainRequester(t *testing.T) {
	re, _, cleanup := newTestRedis(t)
	defer cleanup()

	cached := new(dns.Msg)
	cached.SetQuestion("example.com.", dns.TypeA)
	cached.Response = true
	cached.AuthenticatedData = true
	rr, _ := dns.NewRR("example.com. 60 IN A 192.0.2.1")
	cached.Answer = []dns.RR{rr}

	k := keyer{prefix: "cdrc"}.key("example.com.", dns.ClassINET, dns.TypeA, false, false)
	wire, err := ToBytes(cached)
	if err != nil {
		t.Fatalf("ToBytes: %v", err)
	}
	if err := re.Add(context.Background(), k, wire, time.Minute); err != nil {
		t.Fatalf("Add: %v", err)
	}

	re.Next = &fakeNext{}
	q := new(dns.Msg)
	q.SetQuestion("example.com.", dns.TypeA)

	rec := dnstest.NewRecorder(&test.ResponseWriter{})
	if _, err := re.ServeDNS(context.Background(), rec, q); err != nil {
		t.Fatalf("ServeDNS: %v", err)
	}
	if rec.Msg == nil {
		t.Fatal("no message written to client")
	}
	if rec.Msg.AuthenticatedData {
		t.Fatal("served cache copy must clear AD when requester had neither DO nor AD")
	}
}

func TestServeDNS_CacheHit_PreservesADForADRequester(t *testing.T) {
	re, _, cleanup := newTestRedis(t)
	defer cleanup()

	cached := new(dns.Msg)
	cached.SetQuestion("example.com.", dns.TypeA)
	cached.Response = true
	cached.AuthenticatedData = true
	rr, _ := dns.NewRR("example.com. 60 IN A 192.0.2.1")
	cached.Answer = []dns.RR{rr}

	k := keyer{prefix: "cdrc"}.key("example.com.", dns.ClassINET, dns.TypeA, false, false)
	wire, err := ToBytes(cached)
	if err != nil {
		t.Fatalf("ToBytes: %v", err)
	}
	if err := re.Add(context.Background(), k, wire, time.Minute); err != nil {
		t.Fatalf("Add: %v", err)
	}

	re.Next = &fakeNext{}
	q := new(dns.Msg)
	q.SetQuestion("example.com.", dns.TypeA)
	q.AuthenticatedData = true

	rec := dnstest.NewRecorder(&test.ResponseWriter{})
	if _, err := re.ServeDNS(context.Background(), rec, q); err != nil {
		t.Fatalf("ServeDNS: %v", err)
	}
	if rec.Msg == nil {
		t.Fatal("no message written to client")
	}
	if !rec.Msg.AuthenticatedData {
		t.Fatal("served cache copy must preserve AD when requester had AD=1")
	}
}

func TestServeDNS_CacheHit_PreservesADForDORequester(t *testing.T) {
	re, _, cleanup := newTestRedis(t)
	defer cleanup()

	cached := new(dns.Msg)
	cached.SetQuestion("example.com.", dns.TypeA)
	cached.Response = true
	cached.AuthenticatedData = true
	rr, _ := dns.NewRR("example.com. 60 IN A 192.0.2.1")
	cached.Answer = []dns.RR{rr}
	opt := &dns.OPT{Hdr: dns.RR_Header{Name: ".", Rrtype: dns.TypeOPT}}
	opt.SetDo()
	cached.Extra = append(cached.Extra, opt)

	k := keyer{prefix: "cdrc"}.key("example.com.", dns.ClassINET, dns.TypeA, true, false)
	wire, err := ToBytes(cached)
	if err != nil {
		t.Fatalf("ToBytes: %v", err)
	}
	if err := re.Add(context.Background(), k, wire, time.Minute); err != nil {
		t.Fatalf("Add: %v", err)
	}

	re.Next = &fakeNext{}
	q := new(dns.Msg)
	q.SetQuestion("example.com.", dns.TypeA)
	q.Extra = append(q.Extra, opt)

	rec := dnstest.NewRecorder(&test.ResponseWriter{})
	if _, err := re.ServeDNS(context.Background(), rec, q); err != nil {
		t.Fatalf("ServeDNS: %v", err)
	}
	if rec.Msg == nil {
		t.Fatal("no message written to client")
	}
	if !rec.Msg.AuthenticatedData {
		t.Fatal("served cache copy must preserve AD when requester had DO=1")
	}
}

func TestServeDNS_CacheMiss_FallsThrough(t *testing.T) {
	re, _, cleanup := newTestRedis(t)
	defer cleanup()

	next := &fakeNext{rcode: dns.RcodeSuccess}
	re.Next = next

	q := new(dns.Msg)
	q.SetQuestion("missing.example.com.", dns.TypeA)

	rec := dnstest.NewRecorder(&test.ResponseWriter{})
	if _, err := re.ServeDNS(context.Background(), rec, q); err != nil {
		t.Fatalf("ServeDNS: %v", err)
	}
	if !next.called {
		t.Fatal("Next was not invoked on a cache miss")
	}
}

func TestServeDNS_UpstreamMiss_PopulatesCacheAsync(t *testing.T) {
	// On a cache miss the upstream reply is written back asynchronously, so
	// the next request for the same name hits Redis instead of forwarding
	// upstream again. Polls Redis (the SET runs in a fire-and-forget
	// goroutine, so the key may not be present the instant ServeDNS returns).
	re, mr, cleanup := newTestRedis(t)
	defer cleanup()

	answer, _ := dns.NewRR("example.com. 60 IN A 192.0.2.1")
	re.Next = &fakeNext{rcode: dns.RcodeSuccess, answer: []dns.RR{answer}}

	q := new(dns.Msg)
	q.SetQuestion("example.com.", dns.TypeA)
	rec := dnstest.NewRecorder(&test.ResponseWriter{})
	if _, err := re.ServeDNS(context.Background(), rec, q); err != nil {
		t.Fatalf("ServeDNS: %v", err)
	}

	k := keyer{prefix: "cdrc"}.key("example.com.", dns.ClassINET, dns.TypeA, false, false)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if mr.Exists(k) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected key %q to be populated by the async cache write", k)
}

func TestServeDNS_AsyncWriteSurvivesClientCancel(t *testing.T) {
	// Even if the client cancels the request after the upstream reply, the
	// cache write must still land — that's the whole point of doing it on a
	// detached context (future requesters get a hit instead of re-burdening
	// upstream).
	re, mr, cleanup := newTestRedis(t)
	defer cleanup()

	answer, _ := dns.NewRR("late.example.com. 60 IN A 192.0.2.2")
	re.Next = &fakeNext{rcode: dns.RcodeSuccess, answer: []dns.RR{answer}}

	q := new(dns.Msg)
	q.SetQuestion("late.example.com.", dns.TypeA)

	ctx, cancel := context.WithCancel(context.Background())
	rec := dnstest.NewRecorder(&test.ResponseWriter{})
	if _, err := re.ServeDNS(ctx, rec, q); err != nil {
		t.Fatalf("ServeDNS: %v", err)
	}
	cancel() // simulate client going away after the response was written

	k := keyer{prefix: "cdrc"}.key("late.example.com.", dns.ClassINET, dns.TypeA, false, false)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if mr.Exists(k) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("cache write was lost when the client cancelled (expected detached ctx to keep it alive)")
}

func TestServeDNS_QuestionMismatch_TreatedAsMiss(t *testing.T) {
	// If a stored entry's question does not match the request, the plugin
	// must not serve it — it should fall through to Next and bump
	// cacheCollisions. Simulated by SETing a message for one name under
	// the cache key of another name.
	re, _, cleanup := newTestRedis(t)
	defer cleanup()

	wrong := new(dns.Msg)
	wrong.SetQuestion("attacker.example.com.", dns.TypeA)
	wrong.Response = true
	rr, _ := dns.NewRR("attacker.example.com. 60 IN A 198.51.100.1")
	wrong.Answer = []dns.RR{rr}

	// Store under the key of victim.example.com. — what a hash collision or
	// version-skew bug would look like.
	victimKey := keyer{prefix: "cdrc"}.key("victim.example.com.", dns.ClassINET, dns.TypeA, false, false)
	wire, err := ToBytes(wrong)
	if err != nil {
		t.Fatalf("ToBytes: %v", err)
	}
	if err := re.Add(context.Background(), victimKey, wire, time.Minute); err != nil {
		t.Fatalf("Add: %v", err)
	}

	next := &fakeNext{rcode: dns.RcodeSuccess}
	re.Next = next

	q := new(dns.Msg)
	q.SetQuestion("victim.example.com.", dns.TypeA)

	rec := dnstest.NewRecorder(&test.ResponseWriter{})
	if _, err := re.ServeDNS(context.Background(), rec, q); err != nil {
		t.Fatalf("ServeDNS: %v", err)
	}
	if !next.called {
		t.Fatal("Next was not invoked: plugin served a mismatched cached entry")
	}
	// rec.Msg should reflect Next's reply, not the attacker's record.
	if rec.Msg != nil && len(rec.Msg.Answer) > 0 {
		ans := rec.Msg.Answer[0].String()
		if strings.Contains(ans, "198.51.100.1") {
			t.Fatalf("attacker IP leaked through: %s", ans)
		}
	}
}

func TestGet_KeyComponentMismatch_SelfHeals(t *testing.T) {
	// Every component fed into keyer.key (qname, qtype, qclass, DO, CD) must
	// be re-verified after GET — a cached entry that differs in any one of
	// them is a collision/corruption signal and must be dropped + evicted,
	// not served. Tested at the re.get level to avoid racing with ServeDNS's
	// own cache-repopulation write on the same key.
	//
	// Each subtest stores a "wrong" message that matches the victim in four
	// of five key components under the victim's key — what a true 64-bit
	// hash collision would look like at the read path.
	const victimName = "victim.example.com."

	withOPT := func(m *dns.Msg, do bool) *dns.Msg {
		opt := &dns.OPT{Hdr: dns.RR_Header{Name: ".", Rrtype: dns.TypeOPT}}
		if do {
			opt.SetDo()
		}
		m.Extra = append(m.Extra, opt)
		return m
	}
	mkMsg := func(name string, qtype, qclass uint16, do, cd bool, rrText string) *dns.Msg {
		m := new(dns.Msg)
		m.Response = true
		m.CheckingDisabled = cd
		m.Question = []dns.Question{{Name: name, Qtype: qtype, Qclass: qclass}}
		rr, _ := dns.NewRR(rrText)
		m.Answer = []dns.RR{rr}
		return withOPT(m, do)
	}

	cases := []struct {
		name        string
		victimType  uint16
		victimClass uint16
		victimDO    bool
		victimCD    bool
		stored      *dns.Msg
	}{
		{
			name:        "qname",
			victimType:  dns.TypeA,
			victimClass: dns.ClassINET,
			stored:      mkMsg("attacker.example.com.", dns.TypeA, dns.ClassINET, false, false, "attacker.example.com. 60 IN A 198.51.100.1"),
		},
		{
			name:        "qtype",
			victimType:  dns.TypeA,
			victimClass: dns.ClassINET,
			stored:      mkMsg(victimName, dns.TypeAAAA, dns.ClassINET, false, false, "victim.example.com. 60 IN AAAA 2001:db8::1"),
		},
		{
			name:        "qclass",
			victimType:  dns.TypeA,
			victimClass: dns.ClassINET,
			stored:      mkMsg(victimName, dns.TypeA, dns.ClassCHAOS, false, false, "victim.example.com. 60 CH A 198.51.100.1"),
		},
		{
			name:        "do",
			victimType:  dns.TypeA,
			victimClass: dns.ClassINET,
			stored:      mkMsg(victimName, dns.TypeA, dns.ClassINET, true, false, "victim.example.com. 60 IN A 198.51.100.1"),
		},
		{
			name:        "cd",
			victimType:  dns.TypeA,
			victimClass: dns.ClassINET,
			stored:      mkMsg(victimName, dns.TypeA, dns.ClassINET, false, true, "victim.example.com. 60 IN A 198.51.100.1"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			re, mr, cleanup := newTestRedis(t)
			defer cleanup()

			victimKey := keyer{prefix: "cdrc"}.key(victimName, tc.victimClass, tc.victimType, tc.victimDO, tc.victimCD)
			wire, err := ToBytes(tc.stored)
			if err != nil {
				t.Fatalf("ToBytes: %v", err)
			}
			if err := re.Add(context.Background(), victimKey, wire, time.Minute); err != nil {
				t.Fatalf("Add: %v", err)
			}

			q := new(dns.Msg)
			q.SetQuestion(victimName, tc.victimType)
			q.Question[0].Qclass = tc.victimClass
			q.CheckingDisabled = tc.victimCD
			withOPT(q, tc.victimDO)
			state := request.Request{W: &test.ResponseWriter{}, Req: q}
			if m := re.get(context.Background(), state, ""); m != nil {
				t.Fatalf("expected nil on %s mismatch, got %#v", tc.name, m)
			}

			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				if !mr.Exists(victimKey) {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Fatalf("expected key %q to be evicted by self-heal after %s mismatch, but it persisted", victimKey, tc.name)
		})
	}
}

// TestWriteMsg_KeysDOFromResponse pins that the DO bit in the key comes from the
// reply, not the request. A validating forwarder can answer a DO=0 client with a
// DO=1 reply; that reply carries DNSSEC records, so it must be filed under the
// DO=1 slot (reusable by DO=1 clients) and must NOT be reachable under the DO=0
// slot the originating client looks up — a DO=0 client must never be served
// RRSIGs (RFC 4035 §3.2.1).
func TestWriteMsg_KeysDOFromResponse(t *testing.T) {
	w, _, cleanup := writeMsgFixture(t, "example.com.", dns.TypeA, time.Minute, 0, time.Minute, 0)
	defer cleanup()

	// Request is DO=0 (the fixture sends no OPT). Build a DO=1 reply.
	res := new(dns.Msg)
	res.SetReply(w.state.Req)
	rr, _ := dns.NewRR("example.com. 60 IN A 192.0.2.1")
	res.Answer = []dns.RR{rr}
	opt := &dns.OPT{Hdr: dns.RR_Header{Name: ".", Rrtype: dns.TypeOPT}}
	opt.SetDo()
	res.Extra = append(res.Extra, opt)

	if err := w.WriteMsg(res); err != nil {
		t.Fatalf("WriteMsg: %v", err)
	}

	do1Key := w.keyer.key("example.com.", dns.ClassINET, dns.TypeA, true, false)
	do0Key := w.keyer.key("example.com.", dns.ClassINET, dns.TypeA, false, false)

	// The async SET must land under the DO=1 key (the response's DO bit).
	deadline := time.Now().Add(2 * time.Second)
	var got *dns.Msg
	for time.Now().Before(deadline) {
		if m, _, err := w.Get(context.Background(), do1Key); err == nil && m != nil {
			got = m
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got == nil {
		t.Fatal("expected entry under the DO=1 key (response DO), found none")
	}
	// The DO=0 slot — what the originating DO=0 client would look up — must stay
	// empty, so that client cleanly misses rather than being served RRSIGs.
	if m, _, err := w.Get(context.Background(), do0Key); err != nil || m != nil {
		t.Fatalf("DO=0 slot must be empty, got msg=%v err=%v", m, err)
	}
}

func TestGet_DecodeError_SelfHeals(t *testing.T) {
	// A corrupt wire-bytes value must be evicted so the next read does
	// not keep tripping the same decode error until natural TTL. Tested
	// at the Redis.Get level to avoid racing with ServeDNS's own
	// cache-repopulation write on the same key.
	re, mr, cleanup := newTestRedis(t)
	defer cleanup()

	const k = "corrupt-key"
	mr.Set(k, "\x00\x01\x02") // shorter than the 12-byte DNS header
	mr.SetTTL(k, time.Minute)

	out, _, err := re.Get(context.Background(), k)
	if err == nil {
		t.Fatalf("expected decode error, got msg=%#v", out)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !mr.Exists(k) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected corrupt key %q to be evicted by self-heal", k)
}

func TestGet_EmptyValue_SelfHeals(t *testing.T) {
	// Empty values shouldn't exist post-encode-error fix, but if some
	// older version (or external SET) parked one in Redis we must evict
	// rather than treat it as a sticky miss.
	re, mr, cleanup := newTestRedis(t)
	defer cleanup()

	const k = "rogue-empty"
	mr.Set(k, "")
	mr.SetTTL(k, time.Minute)

	out, _, err := re.Get(context.Background(), k)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if out != nil {
		t.Fatalf("expected nil msg for empty value, got %#v", out)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !mr.Exists(k) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected empty-value key to be evicted by self-heal")
}

func TestServeDNS_DecodeErrorTreatedAsMiss(t *testing.T) {
	// A corrupt Redis value must not be served as an empty reply — the
	// read error propagates and the chain falls through to Next.
	re, mr, cleanup := newTestRedis(t)
	defer cleanup()

	k := keyer{prefix: "cdrc"}.key("example.com.", dns.ClassINET, dns.TypeA, false, false)
	mr.Set(k, "\x00\x01\x02") // shorter than DNS header — unpack fails

	next := &fakeNext{rcode: dns.RcodeSuccess}
	re.Next = next

	q := new(dns.Msg)
	q.SetQuestion("example.com.", dns.TypeA)

	rec := dnstest.NewRecorder(&test.ResponseWriter{})
	if _, err := re.ServeDNS(context.Background(), rec, q); err != nil {
		t.Fatalf("ServeDNS: %v", err)
	}
	if !next.called {
		t.Fatal("Next was not invoked: corrupt cache entry was served as a hit")
	}
}

func TestErrReason(t *testing.T) {
	// Each go-redis error class must map to the documented bucket so the
	// {get,set}_errors_total reason label is meaningful to operators.
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"deadline_exceeded", context.DeadlineExceeded, "timeout"},
		{"canceled", context.Canceled, "timeout"},
		{"deadline_wrapped", fmt.Errorf("op failed: %w", context.DeadlineExceeded), "timeout"},
		{"pool_timeout", redis.ErrPoolTimeout, "timeout"},
		{"net_timeout", &net.OpError{Op: "read", Err: &timeoutNetErr{}}, "timeout"},
		{"net_refused", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, "connection"},
		{"raw_eof", io.EOF, "connection"},
		{"unexpected_eof", io.ErrUnexpectedEOF, "connection"},
		{"wrapped_eof", fmt.Errorf("redis: read: %w", io.EOF), "connection"},
		{"plain_error", errors.New("NOAUTH Authentication required"), "other"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := errReason(tc.err); got != tc.want {
				t.Errorf("errReason(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

// timeoutNetErr implements net.Error with Timeout()==true so net_timeout
// case can exercise the net.Error branch without opening a real socket.
type timeoutNetErr struct{}

func (timeoutNetErr) Error() string   { return "i/o timeout" }
func (timeoutNetErr) Timeout() bool   { return true }
func (timeoutNetErr) Temporary() bool { return true }

// TestErrReason_RealClientErrors verifies the classifier against the actual
// error values that the go-redis v9 client produces in the failure modes that
// matter operationally — synthetic shapes in TestErrReason can drift from
// what the dependency actually returns. Each subtest provokes a specific
// class of failure and asserts both the classifier bucket and a relevant
// invariant on the underlying error (Is/As/text), so a future go-redis
// upgrade that reshapes errors fails loudly here rather than silently
// mis-bucketing in production.
func TestErrReason_RealClientErrors(t *testing.T) {
	t.Run("connection_refused", func(t *testing.T) {
		// Dial a port nothing listens on. go-redis wraps this in *net.OpError;
		// classifier should see net.Error with Timeout()==false → connection.
		client := redis.NewClient(&redis.Options{
			Addr:        "127.0.0.1:1",
			DialTimeout: 500 * time.Millisecond,
			MaxRetries:  -1,
		})
		defer client.Close()

		err := client.Set(context.Background(), "k", "v", time.Minute).Err()
		if err == nil {
			t.Fatal("expected error dialing 127.0.0.1:1, got nil")
		}
		var netErr net.Error
		if !errors.As(err, &netErr) {
			t.Errorf("expected net.Error in chain, got %T: %v", err, err)
		}
		if got := errReason(err); got != "connection" {
			t.Errorf("errReason = %q, want connection (err=%v)", got, err)
		}
	})

	t.Run("context_deadline", func(t *testing.T) {
		mr := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
		defer client.Close()

		// Already-expired context. go-redis returns context.DeadlineExceeded
		// directly from the pool wait / network deadline.
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()

		err := client.Set(ctx, "k", "v", time.Minute).Err()
		if err == nil {
			t.Fatal("expected deadline error, got nil")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded in chain, got %T: %v", err, err)
		}
		if got := errReason(err); got != "timeout" {
			t.Errorf("errReason = %q, want timeout (err=%v)", got, err)
		}
	})

	t.Run("context_canceled", func(t *testing.T) {
		mr := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
		defer client.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := client.Set(ctx, "k", "v", time.Minute).Err()
		if err == nil {
			t.Fatal("expected canceled error, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled in chain, got %T: %v", err, err)
		}
		if got := errReason(err); got != "timeout" {
			t.Errorf("errReason = %q, want timeout (err=%v)", got, err)
		}
	})

	t.Run("server_closed_midflight", func(t *testing.T) {
		// Connect, succeed once, then close the server. The next op sees the
		// connection drop — the exact error shape varies (io.EOF, ECONNRESET,
		// *net.OpError), but all three are connection-level, not application.
		mr := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
		defer client.Close()

		if err := client.Set(context.Background(), "k", "v", time.Minute).Err(); err != nil {
			t.Fatalf("priming SET failed: %v", err)
		}
		mr.Close()

		// Retry briefly because the first op may race the server shutdown
		// (pool keepalive can race the FIN). Any non-nil error within the
		// window must classify as connection.
		var err error
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			err = client.Get(context.Background(), "k").Err()
			if err != nil && err != redis.Nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if err == nil || err == redis.Nil {
			t.Fatalf("expected connection error after server.Close, got %v", err)
		}
		if got := errReason(err); got != "connection" {
			t.Errorf("errReason = %q, want connection (err=%T: %v)", got, err, err)
		}
	})

	t.Run("resp_error", func(t *testing.T) {
		// A WRONGTYPE error: SET a string, then call INCR on it. miniredis
		// returns the canonical RESP-level "-WRONGTYPE ..." which go-redis
		// surfaces as a plain error (not net.Error, not context).
		mr := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
		defer client.Close()

		if err := client.Set(context.Background(), "k", "not-a-number", time.Minute).Err(); err != nil {
			t.Fatalf("priming SET failed: %v", err)
		}
		err := client.Incr(context.Background(), "k").Err()
		if err == nil {
			t.Fatal("expected RESP WRONGTYPE error, got nil")
		}
		var netErr net.Error
		if errors.As(err, &netErr) {
			t.Errorf("RESP error should not satisfy net.Error, but did: %T", err)
		}
		if got := errReason(err); got != "other" {
			t.Errorf("errReason = %q, want other (err=%v)", got, err)
		}
	})

	t.Run("pool_timeout", func(t *testing.T) {
		// Pool of 1 with PoolTimeout=10ms. Block the only connection by
		// reserving it (BLPOP on an empty key in a goroutine), then a
		// concurrent SET must fail with redis.ErrPoolTimeout.
		mr := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{
			Addr:        mr.Addr(),
			PoolSize:    1,
			PoolTimeout: 50 * time.Millisecond,
			MaxRetries:  -1,
		})
		defer client.Close()

		// Hold the only connection inside a BLPOP — miniredis honours BLPOP
		// timeouts, so the goroutine releases the conn after `block` seconds.
		held := make(chan struct{})
		go func() {
			close(held)
			_ = client.BLPop(context.Background(), 2*time.Second, "never").Err()
		}()
		<-held
		// Give the BLPOP a moment to actually occupy the pool slot.
		time.Sleep(50 * time.Millisecond)

		err := client.Set(context.Background(), "k", "v", time.Minute).Err()
		if err == nil {
			t.Fatal("expected pool timeout, got nil")
		}
		if !errors.Is(err, redis.ErrPoolTimeout) {
			t.Errorf("expected redis.ErrPoolTimeout in chain, got %T: %v", err, err)
		}
		if got := errReason(err); got != "timeout" {
			t.Errorf("errReason = %q, want timeout (err=%v)", got, err)
		}
	})
}
