package redis_cache

import (
	"time"

	"github.com/coredns/coredns/plugin/pkg/response"

	"github.com/miekg/dns"
)

func minMsgTTL(m *dns.Msg, mt response.Type) time.Duration {
	if mt != response.NoError && mt != response.NameError && mt != response.NoData {
		return 0
	}

	// No data to examine, return a short TTL as a fail safe.
	if len(m.Answer)+len(m.Ns) == 0 {
		return failSafeTTL
	}

	// Iterate Answer / Ns / Extra in sequence rather than concatenating —
	// append(append(m.Answer, m.Ns...), m.Extra...) would risk writing into
	// m.Answer's backing array if it has spare capacity, and allocates two
	// throwaway slices on the cache hot path.
	minTTL := maxTTL
	for _, section := range [...][]dns.RR{m.Answer, m.Ns, m.Extra} {
		for _, r := range section {
			if r.Header().Rrtype == dns.TypeOPT {
				// OPT records use TTL field for extended rcode and flags.
				continue
			}
			switch mt {
			case response.NameError, response.NoData:
				if r.Header().Rrtype == dns.TypeSOA {
					return time.Duration(r.(*dns.SOA).Minttl) * time.Second
				}
			case response.NoError, response.Delegation:
				if r.Header().Ttl < uint32(minTTL.Seconds()) {
					minTTL = time.Duration(r.Header().Ttl) * time.Second
				}
			}
		}
	}
	return minTTL
}

func setMsgTTL(m *dns.Msg, ttl int) {
	for i := range m.Answer {
		m.Answer[i].Header().Ttl = uint32(ttl)
	}
	for i := range m.Ns {
		m.Ns[i].Header().Ttl = uint32(ttl)
	}
	for i := range m.Extra {
		if m.Extra[i].Header().Rrtype != dns.TypeOPT {
			m.Extra[i].Header().Ttl = uint32(ttl)
		}
	}
}
