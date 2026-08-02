package redis_cache

import (
	"context"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/metrics"
	"github.com/coredns/coredns/request"

	"github.com/miekg/dns"
)

// ServeDNS implements the plugin.Handler interface.
func (re *Redis) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	state := request.Request{W: w, Req: r}

	zone := plugin.Zones(re.Zones).Matches(state.Name())
	if zone == "" {
		return plugin.NextOrFailure(re.Name(), re.Next, ctx, w, r)
	}

	server := metrics.WithServer(ctx)

	m := re.get(ctx, state, server)
	if m != nil {
		// SetReply forces Rcode to RcodeSuccess (miekg/dns defaults.go), which
		// would turn a cached NXDOMAIN into NODATA on the wire. Snapshot the
		// cached Rcode and restore it after SetReply has copied the request ID /
		// question / flags across. This mirrors CoreDNS's own cache plugin,
		// which does the same save/restore in item.toMsg (plugin/cache/item.go:
		// m1.SetReply(m) followed by m1.Rcode = i.Rcode).
		rcode := m.Rcode
		m.SetReply(r)
		m.Rcode = rcode
		// Force the Authoritative bit on, matching CoreDNS's own cache plugin
		// (plugin/cache/item.go toMsg). Strictly a cache is not authoritative,
		// but some legacy stub resolvers — notably glibc getaddrinfo on older
		// systems — discard replies that lack AA, so a served cache copy sets it
		// to 1 regardless of the stored value.
		m.Authoritative = true
		if !state.Do() && !r.AuthenticatedData {
			m.AuthenticatedData = false
		}
		// Return the cached Rcode so NXDOMAIN/SERVFAIL show up correctly in
		// dnstap/metrics, and propagate any WriteMsg error.
		err := w.WriteMsg(m)
		return m.Rcode, err
	}

	crr := &ResponseWriter{
		ResponseWriter: w,
		Redis:          re,
		state:          state,
		server:         server,
		ctx:            ctx,
	}
	return plugin.NextOrFailure(re.Name(), re.Next, ctx, crr, r)
}

// Name implements the Handler interface.
func (re *Redis) Name() string { return "redis_cache" }
