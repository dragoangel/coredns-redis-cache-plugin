package redis_cache

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
	"github.com/redis/go-redis/v9"
)

// Redis is a plugin that looks up responses in a Redis cache and caches replies.
// It has a success and a denial of existence cache.
type Redis struct {
	Next  plugin.Handler
	Zones []string

	// writeClient routes to the master (or standalone) for SET operations.
	writeClient redis.UniversalClient
	// readClient routes to a replica (or the same client) for GET operations.
	// Used for standalone, sentinel, cluster, and the explicit-replicas mode
	// when there is exactly one read endpoint. nil when readPool is in use.
	readClient redis.UniversalClient
	// readPool round-robins (random pick) GETs across N>=2 explicit read
	// replicas. nil for all other modes.
	readPool *readReplicaPool

	pMaxTTL time.Duration // max TTL for positive (success) responses
	nMaxTTL time.Duration // max TTL for negative (denial) responses
	pMinTTL time.Duration // min TTL for positive responses (floor)
	nMinTTL time.Duration // min TTL for negative responses (floor)

	// Connection config
	addr          string   // standalone write endpoint (default 127.0.0.1:6379)
	endpointSet   bool     // true if 'endpoint' was set explicitly (used to detect mode conflicts)
	username      string   // ACL username for the data plane (primary/replicas/cluster), Redis 6+, optional
	password      string   // AUTH password for the data plane, optional
	readEndpoints []string // explicit read-only replica endpoints

	// Timeouts
	connectTimeout time.Duration // TCP dial timeout (default 1s)
	readTimeout    time.Duration // per-command read timeout (default 1s)
	writeTimeout   time.Duration // per-command write timeout (default 2s)

	// Database index (0 = default DB; only meaningful in non-cluster modes)
	db int

	// Connection pool — all 0 = leave at go-redis default (see README)
	poolSize        int           // PoolSize: max sockets per client
	minIdleConns    int           // MinIdleConns: warm pool floor
	maxIdleConns    int           // MaxIdleConns: cap on idle sockets
	maxActiveConns  int           // MaxActiveConns: hard cap on total open sockets
	connMaxIdleTime time.Duration // ConnMaxIdleTime: close after this much idle
	connMaxLifetime time.Duration // ConnMaxLifetime: forced recycle after this lifetime
	poolTimeout     time.Duration // PoolTimeout: how long to wait when pool is exhausted

	// Retries — see New() for plugin default rationale
	maxRetries      int           // go-redis MaxRetries: -1=disabled, N>0=explicit (parser translates user-facing 0 → -1)
	minRetryBackoff time.Duration // MinRetryBackoff between retries (0 = go-redis default 8ms)
	maxRetryBackoff time.Duration // MaxRetryBackoff between retries (0 = go-redis default 512ms)

	// Network tuning
	tcpKeepalive time.Duration // net.Dialer.KeepAlive interval (0 = OS default)

	// DNS resolver for Redis hostnames (bypasses system resolver to avoid circular deps)
	resolver string // DNS server address for resolving Redis hostnames (e.g. "10.96.0.10:53")

	// Sentinel config
	sentinels        []string // sentinel addresses
	masterName       string   // sentinel master name (master group name)
	sentinelUsername string   // ACL username for the Sentinel API, Redis 6.2+, optional
	sentinelPassword string   // AUTH password for the Sentinel API, optional

	// Cluster config
	clusterAddrs []string // cluster seed nodes
	readFrom     string   // replica routing strategy: latency|random|primary ("" = latency)

	// TLS config (shared by data-plane and Sentinel API connections)
	tlsEnabled        bool   // true if any tls* directive was set
	tlsCert           string // PEM-encoded client certificate file (for mTLS)
	tlsKey            string // PEM-encoded client private key file (for mTLS)
	tlsCA             string // PEM-encoded CA file replacing OS trust store, optional
	tlsVerifyChain    bool   // verify the server certificate chain against trust roots (default true)
	tlsVerifyHostname bool   // verify the cert SAN/CN matches the dialed hostname (default true)

	// Testing.
	now func() time.Time
}

// New returns a new initialized Redis with default settings. Only fields whose
// zero value is unsuitable as a default are populated here — everything else
// stays at its struct zero so user-supplied directives win and unset knobs
// flow through to go-redis's own documented defaults (see README).
//
// Plugin-specific default override: maxRetries is set to 1 rather than letting
// go-redis's default of 3 kick in. As an L2 cache the plugin should noop quickly
// on any sustained Redis problem — three retries with backoff can stretch a
// single DNS query to ~half a second of cache-side waiting, which defeats the
// design. One retry absorbs an isolated dropped packet / transient blip without
// amplifying an outage. Operators tune via the `retries max N` directive
// (`max 0` disables retries entirely; the parser maps it to go-redis's -1
// internally so the plugin's 0 means literal "no retries").
func New() *Redis {
	return &Redis{
		Zones:             []string{"."},
		addr:              "127.0.0.1:6379",
		pMaxTTL:           maxTTL,
		nMaxTTL:           maxNTTL,
		connectTimeout:    defaultConnectTimeout,
		readTimeout:       defaultReadTimeout,
		writeTimeout:      defaultWriteTimeout,
		poolTimeout:       defaultPoolTimeout,
		maxRetries:        1, // see docstring above
		tlsVerifyChain:    true,
		tlsVerifyHostname: true,
		now:               time.Now,
	}
}

// connect establishes Redis clients based on configuration.
// It supports three modes:
//  1. Sentinel: automatic master/replica discovery via Redis Sentinel.
//  2. Explicit replicas: endpoint for writes + read_endpoint(s) for reads.
//  3. Standalone: single endpoint for both reads and writes.
//
// dialer returns a custom net.Dialer's DialContext when either a custom
// resolver or a TCP keepalive interval is configured. Otherwise it returns
// nil, letting go-redis fall back to its built-in dialer.
//
// Custom resolver: avoids circular DNS dependencies when node-local-dns
// intercepts the DNS VIP — Redis hostnames are resolved via the upstream
// kube-dns instead of the system resolver.
//
// TCP keepalive: probes idle connections so NAT / load-balancer / sidecar
// timeouts don't silently kill them.
func (re *Redis) dialer() func(ctx context.Context, network, addr string) (net.Conn, error) {
	if re.resolver == "" && re.tcpKeepalive == 0 {
		return nil
	}
	d := &net.Dialer{
		Timeout:   re.connectTimeout,
		KeepAlive: re.tcpKeepalive, // 0 = use Go default keepalive
	}
	if re.resolver != "" {
		d.Resolver = &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
				inner := net.Dialer{Timeout: re.connectTimeout}
				return inner.DialContext(ctx, "udp", re.resolver)
			},
		}
	}
	return d.DialContext
}

func (re *Redis) connect() error {
	ctx := context.Background()
	dial := re.dialer()

	tlsCfg, err := re.buildTLSConfig()
	if err != nil {
		return fmt.Errorf("tls: %w", err)
	}

	switch {
	case len(re.clusterAddrs) > 0:
		// Cluster mode — single client routes by hash slot across shards.
		// Note: Redis Cluster only supports DB 0; re.db is intentionally not
		// passed here, and the parser rejects `db != 0` together with `cluster`.
		opts := &redis.ClusterOptions{
			Addrs:           re.clusterAddrs,
			Username:        re.username,
			Password:        re.password,
			MaxRetries:      re.maxRetries,
			MinRetryBackoff: re.minRetryBackoff,
			MaxRetryBackoff: re.maxRetryBackoff,
			DialTimeout:     re.connectTimeout,
			ReadTimeout:     re.readTimeout,
			WriteTimeout:    re.writeTimeout,
			PoolSize:        re.poolSize,
			MinIdleConns:    re.minIdleConns,
			MaxIdleConns:    re.maxIdleConns,
			MaxActiveConns:  re.maxActiveConns,
			ConnMaxIdleTime: re.connMaxIdleTime,
			ConnMaxLifetime: re.connMaxLifetime,
			PoolTimeout:     re.poolTimeout,
			Dialer:          dial,
			TLSConfig:       tlsCfg,
		}
		switch re.readFrom {
		case "random":
			opts.ReadOnly = true
			opts.RouteRandomly = true
		case "primary":
			// reads stay on primaries
		default: // "" or "latency"
			opts.ReadOnly = true
			opts.RouteByLatency = true
		}
		client := redis.NewClusterClient(opts)
		re.writeClient = client
		re.readClient = client
	case re.masterName != "":
		// Sentinel mode — single FailoverClusterClient handles both writes
		// (auto-routed to the Sentinel-discovered master) and reads
		// (RouteRandomly across the Sentinel-discovered replicas). One
		// Sentinel monitor instead of two, no double `+switch-master`
		// subscription. The "Cluster" in the name refers to ClusterClient's
		// routing machinery, not Redis Cluster: NewFailoverClusterClient
		// supplies a Sentinel-fed ClusterSlots callback, which suppresses
		// CLUSTER NODES / CLUSTER SLOTS / READONLY emissions to the
		// non-cluster Redis nodes (see osscluster.go: readOnly is forced
		// false when ClusterSlots != nil).
		client := redis.NewFailoverClusterClient(&redis.FailoverOptions{
			MasterName:       re.masterName,
			SentinelAddrs:    re.sentinels,
			SentinelUsername: re.sentinelUsername,
			SentinelPassword: re.sentinelPassword,
			Username:         re.username,
			Password:         re.password,
			DB:               re.db,
			RouteRandomly:    true,
			MaxRetries:       re.maxRetries,
			MinRetryBackoff:  re.minRetryBackoff,
			MaxRetryBackoff:  re.maxRetryBackoff,
			DialTimeout:      re.connectTimeout,
			ReadTimeout:      re.readTimeout,
			WriteTimeout:     re.writeTimeout,
			PoolSize:         re.poolSize,
			MinIdleConns:     re.minIdleConns,
			MaxIdleConns:     re.maxIdleConns,
			MaxActiveConns:   re.maxActiveConns,
			ConnMaxIdleTime:  re.connMaxIdleTime,
			ConnMaxLifetime:  re.connMaxLifetime,
			PoolTimeout:      re.poolTimeout,
			Dialer:           dial,
			TLSConfig:        tlsCfg,
		})
		re.writeClient = client
		re.readClient = client
	case len(re.readEndpoints) > 0:
		// Explicit read replica mode
		re.writeClient = redis.NewClient(&redis.Options{
			Addr:            re.addr,
			Username:        re.username,
			Password:        re.password,
			DB:              re.db,
			MaxRetries:      re.maxRetries,
			MinRetryBackoff: re.minRetryBackoff,
			MaxRetryBackoff: re.maxRetryBackoff,
			DialTimeout:     re.connectTimeout,
			ReadTimeout:     re.readTimeout,
			WriteTimeout:    re.writeTimeout,
			PoolSize:        re.poolSize,
			MinIdleConns:    re.minIdleConns,
			MaxIdleConns:    re.maxIdleConns,
			MaxActiveConns:  re.maxActiveConns,
			ConnMaxIdleTime: re.connMaxIdleTime,
			ConnMaxLifetime: re.connMaxLifetime,
			PoolTimeout:     re.poolTimeout,
			Dialer:          dial,
			TLSConfig:       tlsCfg,
		})
		// One read endpoint → single client; ≥2 → readReplicaPool (random pick per GET).
		if len(re.readEndpoints) == 1 {
			re.readClient = redis.NewClient(&redis.Options{
				Addr:            re.readEndpoints[0],
				Username:        re.username,
				Password:        re.password,
				DB:              re.db,
				MaxRetries:      re.maxRetries,
				MinRetryBackoff: re.minRetryBackoff,
				MaxRetryBackoff: re.maxRetryBackoff,
				DialTimeout:     re.connectTimeout,
				ReadTimeout:     re.readTimeout,
				WriteTimeout:    re.writeTimeout,
				PoolSize:        re.poolSize,
				MinIdleConns:    re.minIdleConns,
				MaxIdleConns:    re.maxIdleConns,
				MaxActiveConns:  re.maxActiveConns,
				ConnMaxIdleTime: re.connMaxIdleTime,
				ConnMaxLifetime: re.connMaxLifetime,
				PoolTimeout:     re.poolTimeout,
				Dialer:          dial,
				TLSConfig:       tlsCfg,
			})
		} else {
			// Build a per-replica client pool that picks a random replica per GET.
			clients := make([]*redis.Client, len(re.readEndpoints))
			for i, ep := range re.readEndpoints {
				clients[i] = redis.NewClient(&redis.Options{
					Addr:            ep,
					Username:        re.username,
					Password:        re.password,
					DB:              re.db,
					MaxRetries:      re.maxRetries,
					MinRetryBackoff: re.minRetryBackoff,
					MaxRetryBackoff: re.maxRetryBackoff,
					DialTimeout:     re.connectTimeout,
					ReadTimeout:     re.readTimeout,
					WriteTimeout:    re.writeTimeout,
					PoolSize:        re.poolSize,
					MinIdleConns:    re.minIdleConns,
					MaxIdleConns:    re.maxIdleConns,
					MaxActiveConns:  re.maxActiveConns,
					ConnMaxIdleTime: re.connMaxIdleTime,
					ConnMaxLifetime: re.connMaxLifetime,
					PoolTimeout:     re.poolTimeout,
					Dialer:          dial,
					TLSConfig:       tlsCfg,
				})
			}
			re.readPool = &readReplicaPool{clients: clients}
		}
	default:
		// Standalone mode — single client for both reads and writes
		client := redis.NewClient(&redis.Options{
			Addr:            re.addr,
			Username:        re.username,
			Password:        re.password,
			DB:              re.db,
			MaxRetries:      re.maxRetries,
			MinRetryBackoff: re.minRetryBackoff,
			MaxRetryBackoff: re.maxRetryBackoff,
			DialTimeout:     re.connectTimeout,
			ReadTimeout:     re.readTimeout,
			WriteTimeout:    re.writeTimeout,
			PoolSize:        re.poolSize,
			MinIdleConns:    re.minIdleConns,
			MaxIdleConns:    re.maxIdleConns,
			MaxActiveConns:  re.maxActiveConns,
			ConnMaxIdleTime: re.connMaxIdleTime,
			ConnMaxLifetime: re.connMaxLifetime,
			PoolTimeout:     re.poolTimeout,
			Dialer:          dial,
			TLSConfig:       tlsCfg,
		})
		re.writeClient = client
		re.readClient = client
	}

	// Verify connectivity
	if err := re.writeClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("write endpoint: %w", err)
	}
	if re.readClient != nil && re.readClient != re.writeClient {
		if err := re.readClient.Ping(ctx).Err(); err != nil {
			log.Warningf("Read endpoint ping failed (will retry on demand): %s", err)
		}
	}
	if re.readPool != nil {
		re.readPool.ping(ctx)
	}

	return nil
}

// close shuts down Redis clients.
func (re *Redis) close() error {
	var errs []error
	if re.writeClient != nil {
		if err := re.writeClient.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if re.readClient != nil && re.readClient != re.writeClient {
		if err := re.readClient.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if re.readPool != nil {
		if err := re.readPool.close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}
	return nil
}

// Add stores already-serialized wire bytes under the given key in Redis with
// the specified duration. Writes always go to the master/write client.
// Serialization is the caller's responsibility (see set()) so that pack errors
// and Redis-side errors can be reported on different metrics.
func (re *Redis) Add(ctx context.Context, key string, wire []byte, duration time.Duration) error {
	return re.writeClient.Set(ctx, key, wire, duration).Err()
}

// readPipeline opens a pipeline on the read side: a randomly-picked replica
// from readPool when multi-replica mode is configured, otherwise the single
// readClient (standalone, sentinel, cluster, or one-replica explicit mode).
func (re *Redis) readPipeline() redis.Pipeliner {
	if re.readPool != nil {
		return re.readPool.pipeline()
	}
	return re.readClient.Pipeline()
}

// Get retrieves a cached DNS message by key from a read replica. Returns:
//   - (msg, nil)  on a cache hit
//   - (nil, nil)  on a cache miss (key not present in Redis)
//   - (nil, err)  on a read error (network, timeout, protocol)
func (re *Redis) Get(ctx context.Context, key string) (*dns.Msg, error) {
	// Pipeline GET and TTL in a single round-trip.
	pipe := re.readPipeline()
	getCmd := pipe.Get(ctx, key)
	ttlCmd := pipe.TTL(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	b, err := getCmd.Bytes()
	if err == redis.Nil {
		return nil, nil // miss
	}
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		// Should not happen ever, but if faced, evict so we don't keep
		// handling the same empty value as a miss until natural TTL.
		re.evictAsync(ctx, key)
		return nil, nil
	}

	ttl := 0
	if d, err := ttlCmd.Result(); err == nil && d > 0 {
		ttl = int(d.Seconds())
	}

	m, err := FromBytes(b, ttl)
	if err != nil {
		// Corrupt wire bytes in Redis — self-heal so subsequent reads
		// don't keep tripping the same decode error.
		re.evictAsync(ctx, key)
		return nil, err
	}
	return m, nil
}

// evictAsync schedules a non-blocking EXPIRE 0 (detached ctx, not DEL —
// keeps Redis's main thread free) on a detected-broken entry. Without it,
// every hit on the orphan re-trips the same bad read and bumps its error
// counter — one bad key would read as a fleet-wide incident in monitoring.
func (re *Redis) evictAsync(parent context.Context, key string) {
	go func() {
		if err := re.writeClient.Expire(context.WithoutCancel(parent), key, 0).Err(); err != nil {
			log.Debugf("Failed to evict cache key %s: %s", key, err)
		}
	}()
}

// get looks up a cached response for the given request. After fetching, the
// cached message's question is verified to match the request as defense in
// depth against corrupted entries or version-skewed encodings; a mismatch is
// reported via cacheCollisions and treated as a miss.
func (re *Redis) get(ctx context.Context, state request.Request, server string) *dns.Msg {
	k := cacheKey(state.Name(), state.QClass(), state.QType(), state.Do())

	m, err := re.Get(ctx, k)
	if err != nil {
		log.Warningf("Redis cache read error for %s: %s", state.Name(), err)
		cacheReadErrors.WithLabelValues(server).Inc()
		return nil
	}
	if m == nil {
		cacheMisses.WithLabelValues(server).Inc()
		return nil
	}
	if !state.Match(m) || m.Question[0].Qclass != state.QClass() {
		log.Warningf("Redis cache returned mismatched question for %s (got %q type=%d class=%d)",
			state.Name(), m.Question[0].Name, m.Question[0].Qtype, m.Question[0].Qclass)
		cacheCollisions.WithLabelValues(server).Inc()
		// Self-heal: mark the poisoned key as expired so the next request for it
		// gets a clean miss instead of re-tripping the same mismatch until natural TTL.
		re.evictAsync(ctx, k)
		return nil
	}
	log.Debugf("Returning response from Redis cache: %s for %s", m.Question[0].Name, state.Name())
	cacheHits.WithLabelValues(server).Inc()
	return m
}
