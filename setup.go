package redis_cache

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/coredns/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
	clog "github.com/coredns/coredns/plugin/pkg/log"
)

var log = clog.NewWithPlugin("redis_cache")

func init() {
	plugin.Register("redis_cache", setup)

	// If redis_cache is already in dnsserver.Directives, the plugin was compiled in
	// via plugin.cfg (which generates zdirectives.go) and the order is already correct.
	// Nothing more to do.
	for _, d := range dnsserver.Directives {
		if d == "redis_cache" {
			return
		}
	}

	// Out-of-tree usage (e.g. anonymous-imported into a custom binary that doesn't
	// regenerate zdirectives.go): inject "redis_cache" right after "cache" so that
	// the in-process cache acts as L1 and redis_cache as L2.
	for i, d := range dnsserver.Directives {
		if d == "cache" {
			updated := make([]string, 0, len(dnsserver.Directives)+1)
			updated = append(updated, dnsserver.Directives[:i+1]...)
			updated = append(updated, "redis_cache")
			updated = append(updated, dnsserver.Directives[i+1:]...)
			dnsserver.Directives = updated
			return
		}
	}
}

func setup(c *caddy.Controller) error {
	re, err := parse(c)
	if err != nil {
		return plugin.Error("redis_cache", err)
	}
	if err := re.connect(); err != nil {
		log.Warningf("Failed to connect to Redis: %s", err)
	} else {
		mode := fmt.Sprintf("standalone @ %s", re.addr)
		switch {
		case len(re.clusterAddrs) > 0:
			rf := re.readFrom
			if rf == "" {
				rf = "latency"
			}
			mode = fmt.Sprintf("cluster (read_from=%s) seeds: %s", rf, strings.Join(re.clusterAddrs, ", "))
		case re.masterName != "":
			mode = fmt.Sprintf("sentinel (master=%s) via %s", re.masterName, strings.Join(re.sentinels, ", "))
		case len(re.readEndpoints) > 0:
			mode = fmt.Sprintf("primary @ %s, replicas: %s", re.addr, strings.Join(re.readEndpoints, ", "))
		}
		log.Infof("Connected to Redis (%s)", mode)
	}

	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		re.Next = next
		return re
	})

	c.OnShutdown(func() error {
		return re.close()
	})

	return nil
}

func parse(c *caddy.Controller) (*Redis, error) {
	re := New()

	for c.Next() {
		// Positional args, if provided, fully replace the server-block zones
		// (TTL is configured via `success` / `denial` inside the block — there
		// is no inline TTL shorthand). Defaults to the surrounding server-block
		// zones when no positional arg is given. Copy in both cases so the
		// later in-place NormalizeExact() doesn't mutate Caddy's slice.
		var origins []string
		if args := c.RemainingArgs(); len(args) > 0 {
			origins = append([]string(nil), args...)
		} else {
			origins = append([]string(nil), c.ServerBlockKeys...)
		}

		for c.NextBlock() {
			switch c.Val() {
			case Success:
				// success <max_ttl> [<min_ttl>]
				args := c.RemainingArgs()
				if len(args) < 1 {
					return nil, c.ArgErr()
				}
				pttl, err := parseTTL(args[0])
				if err != nil {
					return nil, c.Errf("success: %v", err)
				}
				if pttl <= 0 {
					return nil, fmt.Errorf("success max TTL must be positive, got %s", pttl)
				}
				re.pMaxTTL = pttl
				if len(args) > 1 {
					pmin, err := parseTTL(args[1])
					if err != nil {
						return nil, c.Errf("success min TTL: %v", err)
					}
					if pmin < 0 {
						return nil, fmt.Errorf("success min TTL cannot be negative, got %s", pmin)
					}
					if pmin > pttl {
						return nil, fmt.Errorf("success min TTL (%s) cannot exceed max TTL (%s)", pmin, pttl)
					}
					re.pMinTTL = pmin
				}

			case Denial:
				// denial <max_ttl> [<min_ttl>]
				args := c.RemainingArgs()
				if len(args) < 1 {
					return nil, c.ArgErr()
				}
				nttl, err := parseTTL(args[0])
				if err != nil {
					return nil, c.Errf("denial: %v", err)
				}
				if nttl <= 0 {
					return nil, fmt.Errorf("denial max TTL must be positive, got %s", nttl)
				}
				re.nMaxTTL = nttl
				if len(args) > 1 {
					nmin, err := parseTTL(args[1])
					if err != nil {
						return nil, c.Errf("denial min TTL: %v", err)
					}
					if nmin < 0 {
						return nil, fmt.Errorf("denial min TTL cannot be negative, got %s", nmin)
					}
					if nmin > nttl {
						return nil, fmt.Errorf("denial min TTL (%s) cannot exceed max TTL (%s)", nmin, nttl)
					}
					re.nMinTTL = nmin
				}

			case "endpoint":
				args := c.RemainingArgs()
				if len(args) < 1 {
					return nil, c.ArgErr()
				}
				addr, err := normalizeAddr(args[0])
				if err != nil {
					return nil, err
				}
				re.addr = addr
				re.endpointSet = true

			case "read_endpoint":
				args := c.RemainingArgs()
				if len(args) < 1 {
					return nil, c.ArgErr()
				}
				for _, a := range args {
					addr, err := normalizeAddr(a)
					if err != nil {
						return nil, err
					}
					re.readEndpoints = append(re.readEndpoints, addr)
				}

			case "username":
				args := c.RemainingArgs()
				if len(args) != 1 {
					return nil, c.ArgErr()
				}
				re.username = args[0]

			case "password":
				args := c.RemainingArgs()
				if len(args) != 1 {
					return nil, c.ArgErr()
				}
				re.password = args[0]

			case "sentinel_username":
				args := c.RemainingArgs()
				if len(args) != 1 {
					return nil, c.ArgErr()
				}
				re.sentinelUsername = args[0]

			case "sentinel_password":
				args := c.RemainingArgs()
				if len(args) != 1 {
					return nil, c.ArgErr()
				}
				re.sentinelPassword = args[0]

			case "sentinel":
				args := c.RemainingArgs()
				if len(args) < 2 {
					return nil, fmt.Errorf("sentinel requires MASTER_NAME and at least one sentinel address")
				}
				re.masterName = args[0]
				for _, a := range args[1:] {
					addr, err := normalizeAddr(a)
					if err != nil {
						return nil, fmt.Errorf("invalid sentinel address %q: %w", a, err)
					}
					re.sentinels = append(re.sentinels, addr)
				}

			case "cluster":
				args := c.RemainingArgs()
				if len(args) < 1 {
					return nil, fmt.Errorf("cluster requires at least one seed address")
				}
				for _, a := range args {
					addr, err := normalizeAddr(a)
					if err != nil {
						return nil, fmt.Errorf("invalid cluster seed %q: %w", a, err)
					}
					re.clusterAddrs = append(re.clusterAddrs, addr)
				}

			case "read_from":
				args := c.RemainingArgs()
				if len(args) != 1 {
					return nil, c.ArgErr()
				}
				switch args[0] {
				case "latency", "random", "primary":
					re.readFrom = args[0]
				default:
					return nil, c.Errf("invalid read_from value %q (allowed: latency, random, primary)", args[0])
				}

			case "timeout":
				for c.NextBlock() {
					val := c.Val()
					args := c.RemainingArgs()
					if len(args) != 1 {
						return nil, c.ArgErr()
					}
					d, err := time.ParseDuration(args[0])
					if err != nil {
						return nil, c.Errf("timeout %s: %v", val, err)
					}
					if d <= 0 {
						return nil, fmt.Errorf("timeout %s must be positive: %s", val, d)
					}
					switch val {
					case "connect":
						re.connectTimeout = d
					case "read":
						re.readTimeout = d
					case "write":
						re.writeTimeout = d
					default:
						return nil, c.Errf("unknown timeout directive: %s", val)
					}
				}

			case "pool":
				for c.NextBlock() {
					val := c.Val()
					args := c.RemainingArgs()
					if len(args) != 1 {
						return nil, c.ArgErr()
					}
					switch val {
					case "size", "min_idle", "max_idle", "max_active":
						n, err := strconv.Atoi(args[0])
						if err != nil {
							return nil, err
						}
						if n < 0 {
							return nil, fmt.Errorf("pool %s cannot be negative: %d", val, n)
						}
						switch val {
						case "size":
							re.poolSize = n
						case "min_idle":
							re.minIdleConns = n
						case "max_idle":
							re.maxIdleConns = n
						case "max_active":
							re.maxActiveConns = n
						}
					case "max_idle_time", "max_lifetime", "wait_timeout":
						d, err := time.ParseDuration(args[0])
						if err != nil {
							return nil, c.Errf("pool %s: %v", val, err)
						}
						if d < 0 {
							return nil, fmt.Errorf("pool %s cannot be negative: %s", val, d)
						}
						switch val {
						case "max_idle_time":
							re.connMaxIdleTime = d
						case "max_lifetime":
							re.connMaxLifetime = d
						case "wait_timeout":
							re.poolTimeout = d
						}
					default:
						return nil, c.Errf("unknown pool directive: %s", val)
					}
				}

			case "retries":
				for c.NextBlock() {
					val := c.Val()
					args := c.RemainingArgs()
					if len(args) != 1 {
						return nil, c.ArgErr()
					}
					switch val {
					case "max":
						n, err := strconv.Atoi(args[0])
						if err != nil {
							return nil, c.Errf("retries max: %v", err)
						}
						if n < 0 {
							return nil, c.Errf("retries max cannot be negative: %d", n)
						}
						// User-facing 0 = literal "no retries". go-redis treats 0
						// as "use default (3)", so translate to its -1 disabled
						// sentinel internally.
						if n == 0 {
							re.maxRetries = -1
						} else {
							re.maxRetries = n
						}
					case "min_backoff":
						d, err := time.ParseDuration(args[0])
						if err != nil {
							return nil, c.Errf("retries min_backoff: %v", err)
						}
						if d < 0 {
							return nil, fmt.Errorf("retries min_backoff cannot be negative: %s", d)
						}
						re.minRetryBackoff = d
					case "max_backoff":
						d, err := time.ParseDuration(args[0])
						if err != nil {
							return nil, c.Errf("retries max_backoff: %v", err)
						}
						if d < 0 {
							return nil, fmt.Errorf("retries max_backoff cannot be negative: %s", d)
						}
						re.maxRetryBackoff = d
					default:
						return nil, c.Errf("unknown retries directive: %s", val)
					}
				}

			case "tcp_keepalive":
				args := c.RemainingArgs()
				if len(args) != 1 {
					return nil, c.ArgErr()
				}
				d, err := time.ParseDuration(args[0])
				if err != nil {
					return nil, c.Errf("tcp_keepalive: %v", err)
				}
				if d < 0 {
					return nil, fmt.Errorf("tcp_keepalive cannot be negative: %s", d)
				}
				re.tcpKeepalive = d

			case "db":
				args := c.RemainingArgs()
				if len(args) != 1 {
					return nil, c.ArgErr()
				}
				n, err := strconv.Atoi(args[0])
				if err != nil {
					return nil, err
				}
				if n < 0 {
					return nil, fmt.Errorf("db index cannot be negative: %d", n)
				}
				re.db = n

			case "tls":
				if len(c.RemainingArgs()) > 0 {
					return nil, c.ArgErr()
				}
				re.tlsEnabled = true

			case "tls_cert":
				args := c.RemainingArgs()
				if len(args) != 1 {
					return nil, c.ArgErr()
				}
				re.tlsEnabled = true
				re.tlsCert = args[0]

			case "tls_key":
				args := c.RemainingArgs()
				if len(args) != 1 {
					return nil, c.ArgErr()
				}
				re.tlsEnabled = true
				re.tlsKey = args[0]

			case "tls_ca":
				args := c.RemainingArgs()
				if len(args) != 1 {
					return nil, c.ArgErr()
				}
				re.tlsEnabled = true
				re.tlsCA = args[0]

			case "tls_verify_chain":
				args := c.RemainingArgs()
				if len(args) != 1 {
					return nil, c.ArgErr()
				}
				v, err := parseBool(args[0])
				if err != nil {
					return nil, c.Errf("tls_verify_chain: %v", err)
				}
				re.tlsEnabled = true
				re.tlsVerifyChain = v

			case "tls_verify_hostname":
				args := c.RemainingArgs()
				if len(args) != 1 {
					return nil, c.ArgErr()
				}
				v, err := parseBool(args[0])
				if err != nil {
					return nil, c.Errf("tls_verify_hostname: %v", err)
				}
				re.tlsEnabled = true
				re.tlsVerifyHostname = v

			case "resolver":
				args := c.RemainingArgs()
				if len(args) != 1 {
					return nil, c.ArgErr()
				}
				host, port, err := net.SplitHostPort(args[0])
				if err != nil {
					// No port specified — default to 53
					host = args[0]
					port = "53"
				}
				if port == "" {
					port = "53"
				}
				re.resolver = net.JoinHostPort(host, port)

			default:
				return nil, c.ArgErr()
			}
		}

		// Cross-directive validation. Each topology mode (cluster / sentinel / explicit
		// replicas / standalone) owns a subset of directives; using a directive that
		// belongs to another mode is a configuration error rather than silently ignored.
		switch {
		case len(re.clusterAddrs) > 0:
			if re.masterName != "" {
				return nil, fmt.Errorf("cluster and sentinel modes are mutually exclusive")
			}
			if re.endpointSet {
				return nil, fmt.Errorf("'endpoint' is not used in cluster mode (seed addresses come from the 'cluster' directive)")
			}
			if len(re.readEndpoints) > 0 {
				return nil, fmt.Errorf("'read_endpoint' is not used in cluster mode (cluster handles replica routing internally)")
			}
			if re.db != 0 {
				return nil, fmt.Errorf("'db' must be 0 in cluster mode (Redis Cluster only supports DB 0)")
			}
		case re.masterName != "":
			if re.endpointSet {
				return nil, fmt.Errorf("'endpoint' is not used in sentinel mode (Sentinel discovers the master)")
			}
			if len(re.readEndpoints) > 0 {
				return nil, fmt.Errorf("'read_endpoint' is not used in sentinel mode (Sentinel discovers replicas)")
			}
		default:
			// Standalone or explicit-replica mode.
			if re.sentinelUsername != "" || re.sentinelPassword != "" {
				return nil, fmt.Errorf("'sentinel_username' / 'sentinel_password' require sentinel mode (the 'sentinel' directive)")
			}
			if re.readFrom != "" {
				return nil, fmt.Errorf("'read_from' requires cluster mode")
			}
		}

		if re.minRetryBackoff > 0 && re.maxRetryBackoff > 0 && re.minRetryBackoff > re.maxRetryBackoff {
			return nil, fmt.Errorf("retries min_backoff (%s) cannot exceed max_backoff (%s)", re.minRetryBackoff, re.maxRetryBackoff)
		}

		for i := range origins {
			origins[i] = plugin.Host(origins[i]).NormalizeExact()[0]
		}
		re.Zones = origins

		return re, nil
	}

	return nil, nil
}

// parseBool accepts the common Corefile-style spellings for booleans:
// true/false, on/off, yes/no, 1/0 (case-insensitive).
func parseBool(s string) (bool, error) {
	switch strings.ToLower(s) {
	case "true", "on", "yes", "1":
		return true, nil
	case "false", "off", "no", "0":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean value %q (expected on/off, true/false, yes/no, 1/0)", s)
	}
}

// parseTTL accepts either a Go duration string or a bare integer interpreted
// as seconds (matching CoreDNS's built-in `cache` plugin convention). The
// result must be a whole number of seconds — DNS TTLs are 32-bit second
// counts on the wire and Redis EX takes integer seconds, so sub-second
// granularity has no meaning here and is rejected at parse time.
func parseTTL(s string) (time.Duration, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		n, atoiErr := strconv.Atoi(s)
		if atoiErr != nil {
			return 0, fmt.Errorf("invalid TTL %q (expected a whole-second Go duration like 30s/1h or a bare integer number of seconds)", s)
		}
		d = time.Duration(n) * time.Second
	}
	if d%time.Second != 0 {
		return 0, fmt.Errorf("TTL must be a whole number of seconds, got %s", d)
	}
	return d, nil
}

// normalizeAddr ensures the address has a host:port form, defaulting port to 6379.
// Accepts both IP addresses and hostnames (for Docker/Kubernetes service names).
func normalizeAddr(addr string) (string, error) {
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		if !strings.Contains(err.Error(), "missing port in address") {
			return "", fmt.Errorf("invalid address %q: %w", addr, err)
		}
		// No port specified — default to 6379
		return net.JoinHostPort(addr, "6379"), nil
	}
	if h == "" {
		return "", fmt.Errorf("empty host in address %q", addr)
	}
	if p == "" {
		p = "6379"
	}
	return net.JoinHostPort(h, p), nil
}
