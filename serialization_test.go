package redis_cache

import (
	"testing"

	"github.com/miekg/dns"
)

func TestSerialization_Roundtrip(t *testing.T) {
	in := new(dns.Msg)
	in.SetQuestion("example.com.", dns.TypeA)
	in.Response = true
	rr, err := dns.NewRR("example.com. 60 IN A 192.0.2.1")
	if err != nil {
		t.Fatalf("NewRR: %v", err)
	}
	in.Answer = []dns.RR{rr}

	b, err := ToBytes(in)
	if err != nil {
		t.Fatalf("ToBytes: %v", err)
	}
	out, err := FromBytes(b, 30)
	if err != nil {
		t.Fatalf("FromBytes: %v", err)
	}
	if len(out.Question) != 1 || out.Question[0].Name != "example.com." || out.Question[0].Qtype != dns.TypeA {
		t.Fatalf("question not preserved: %#v", out.Question)
	}
	if len(out.Answer) != 1 {
		t.Fatalf("answer not preserved: %#v", out.Answer)
	}
	if out.Answer[0].Header().Ttl != 30 {
		t.Fatalf("TTL clamp not applied: got %d, want 30", out.Answer[0].Header().Ttl)
	}
}

func TestFromBytes_InvalidWire(t *testing.T) {
	m, err := FromBytes([]byte("garbage-not-dns-wire"), 0)
	if err == nil {
		t.Fatalf("expected error on invalid wire format, got msg=%v", m)
	}
	if m != nil {
		t.Fatalf("expected nil msg on error, got %#v", m)
	}
}

func TestFromBytes_Empty(t *testing.T) {
	m, err := FromBytes(nil, 0)
	if err == nil {
		t.Fatalf("expected error on empty input, got msg=%v", m)
	}
	if m != nil {
		t.Fatalf("expected nil msg on error, got %#v", m)
	}
}

// FuzzFromBytes feeds arbitrary byte slices through the unpack path. It must
// never panic — corrupt or adversarial Redis values are a real possibility
// (older plugin versions, manual SETs, partial writes), and any panic in
// FromBytes would crash the whole CoreDNS process.
//
// Run locally with: go test -run=^$ -fuzz=FuzzFromBytes -fuzztime=30s
func FuzzFromBytes(f *testing.F) {
	// Seed corpus: a valid response, a truncated header, an empty input,
	// and a few well-known short patterns that have tripped DNS parsers
	// historically (compression loops, oversized labels).
	good := new(dns.Msg)
	good.SetQuestion("example.com.", dns.TypeA)
	good.Response = true
	rr, _ := dns.NewRR("example.com. 60 IN A 192.0.2.1")
	good.Answer = []dns.RR{rr}
	if b, err := good.Pack(); err == nil {
		f.Add(b)
		f.Add(b[:5])  // truncated header
		f.Add(b[:12]) // exact header, no data
	}
	f.Add([]byte{})
	f.Add([]byte{0, 0})
	// Compression-pointer loop: name field has a pointer to itself.
	f.Add([]byte{0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0xc0, 0x0c})

	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _ = FromBytes(data, 0)
	})
}

func TestRoundtripPreservesNXDOMAIN(t *testing.T) {
	in := new(dns.Msg)
	in.SetQuestion("nx.example.com.", dns.TypeA)
	in.Response = true
	in.Rcode = dns.RcodeNameError
	soa, _ := dns.NewRR("example.com. 30 IN SOA ns hostmaster 1 60 30 86400 30")
	in.Ns = []dns.RR{soa}

	b, err := ToBytes(in)
	if err != nil {
		t.Fatalf("ToBytes: %v", err)
	}
	if len(b) == 0 {
		t.Fatal("ToBytes produced empty output")
	}
	out, err := FromBytes(b, 0)
	if err != nil {
		t.Fatalf("FromBytes: %v", err)
	}
	if out.Rcode != dns.RcodeNameError {
		t.Fatalf("Rcode lost in roundtrip: got %d", out.Rcode)
	}
}
