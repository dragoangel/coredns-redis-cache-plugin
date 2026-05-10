package redis_cache

import (
	"context"
	"fmt"
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

	k := cacheKey("example.com.", dns.ClassINET, dns.TypeA, false)
	wire, err := ToBytes(in)
	if err != nil {
		t.Fatalf("ToBytes: %v", err)
	}
	if err := re.Add(context.Background(), k, wire, time.Minute); err != nil {
		t.Fatalf("Add: %v", err)
	}

	out, err := re.Get(context.Background(), k)
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
	k := cacheKey(qname, dns.ClassINET, dns.TypeA, false)
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
		m, err := re.Get(context.Background(), k)
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

func TestGet_Miss(t *testing.T) {
	re, _, cleanup := newTestRedis(t)
	defer cleanup()

	out, err := re.Get(context.Background(), "no-such-key")
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

	out, err := re.Get(context.Background(), k)
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

	k := cacheKey("example.com.", dns.ClassINET, dns.TypeA, false)
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

	k := cacheKey("example.com.", dns.ClassINET, dns.TypeA, false)
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

	k := cacheKey("late.example.com.", dns.ClassINET, dns.TypeA, false)
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
	victimKey := cacheKey("victim.example.com.", dns.ClassINET, dns.TypeA, false)
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

func TestGet_QuestionMismatch_SelfHeals(t *testing.T) {
	// After detecting a mismatched cached entry, the plugin must evict it
	// so subsequent reads see a clean miss instead of repeatedly tripping
	// the same collision. Tested at the re.get level to avoid racing with
	// ServeDNS's own cache-repopulation write on the same key.
	re, mr, cleanup := newTestRedis(t)
	defer cleanup()

	wrong := new(dns.Msg)
	wrong.SetQuestion("attacker.example.com.", dns.TypeA)
	wrong.Response = true
	rr, _ := dns.NewRR("attacker.example.com. 60 IN A 198.51.100.1")
	wrong.Answer = []dns.RR{rr}

	victimKey := cacheKey("victim.example.com.", dns.ClassINET, dns.TypeA, false)
	wire, err := ToBytes(wrong)
	if err != nil {
		t.Fatalf("ToBytes: %v", err)
	}
	if err := re.Add(context.Background(), victimKey, wire, time.Minute); err != nil {
		t.Fatalf("Add: %v", err)
	}

	q := new(dns.Msg)
	q.SetQuestion("victim.example.com.", dns.TypeA)
	state := request.Request{W: &test.ResponseWriter{}, Req: q}
	if m := re.get(context.Background(), state, ""); m != nil {
		t.Fatalf("expected nil on question mismatch, got %#v", m)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !mr.Exists(victimKey) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected key %q to be evicted by self-heal, but it persisted", victimKey)
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

	out, err := re.Get(context.Background(), k)
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

	out, err := re.Get(context.Background(), k)
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

	k := cacheKey("example.com.", dns.ClassINET, dns.TypeA, false)
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
