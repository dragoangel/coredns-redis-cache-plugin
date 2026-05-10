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
		m.SetReply(r)
		w.WriteMsg(m)
		return dns.RcodeSuccess, nil
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
