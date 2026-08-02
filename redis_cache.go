package redis_cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
	"golang.org/x/time/rate"
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
	// readPool serves GETs from N>=2 explicit read replicas, picking one at
	// random per call. nil for all other modes.
	readPool *readReplicaPool

	// writeFlight dedupes concurrent Redis SETs for the same cache key.
	// In a thundering-herd scenario (L1 cache plugin disabled, simultaneous
	// upstream forwards for the same name) N goroutines would otherwise
	// each fire their own SET; this collapses them to one without blocking
	// the DNS hot path — the SET still runs in a fire-and-forget goroutine,
	// singleflight just stops the redundant Redis traffic.
	writeFlight singleflight.Group

	// evictFlight dedupes concurrent EXPIRE 0 calls on the same key from
	// the read-side self-heal path (collision / decode error / empty value).
	// Without this, a hot poisoned key triggers an evict goroutine per hit
	// until the first one lands.
	evictFlight singleflight.Group

	// readErrLog throttles the Warningf for Redis read errors to at most
	// one per second. Without throttling, a fleet-scale Redis outage
	// turns into tens of thousands of identical warning lines per second;
	// the cacheReadErrors counter still increments on every error so
	// volume is preserved in metrics.
	readErrLog rate.Sometimes

	pMaxTTL time.Duration // max TTL for positive (success) responses
	nMaxTTL time.Duration // max TTL for negative (denial) responses
	pMinTTL time.Duration // min TTL for positive responses (floor)
	nMinTTL time.Duration // min TTL for negative responses (floor)

	// keyer derives cache keys from DNS question tuples; it holds the namespace
	// prefix and the xxhash seed (see the keyer type). Configured via the
	// key_prefix and key_hash_seed directives.
	keyer keyer

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
		keyer:             keyer{prefix: defaultKeyPrefix},
		readErrLog:        rate.Sometimes{Interval: time.Second},
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

// buildClients constructs the Redis clients for the configured topology.
// It fails only on a non-recoverable configuration error (e.g. an unreadable
// or malformed TLS cert/key/CA file). It performs no I/O: the redis.New*
// constructors only build the client and its connection pool, they never dial,
// so on success writeClient (and readClient or readPool) are guaranteed
// non-nil. Connectivity is checked separately in verifyConnectivity so that a
// Redis that is merely down at boot doesn't get conflated with a bad config.
func (re *Redis) buildClients() error {
	dial := re.dialer()

	tlsCfg, err := re.buildTLSConfig()
	if err != nil {
		return fmt.Errorf("tls: %w", err)
	}

	switch {
	case len(re.clusterAddrs) > 0:
		// Cluster mode — single client routes by hash slot across shards.
		// Redis Cluster supports only DB 0; re.db is dropped here and the
		// parser rejects `db != 0` together with `cluster`.
		opts := re.clusterOptions(dial, tlsCfg)
		applyClusterReadRouting(opts, re.readFrom)
		client := redis.NewClusterClient(opts)
		re.writeClient = client
		re.readClient = client
	case re.masterName != "":
		// Sentinel mode — single FailoverClusterClient handles both writes
		// (auto-routed to the Sentinel-discovered master) and reads
		// (RouteRandomly across the Sentinel-discovered replicas). One
		// Sentinel monitor, one +switch-master subscription. The "Cluster"
		// in the name refers to ClusterClient's routing machinery, not Redis
		// Cluster: NewFailoverClusterClient supplies a Sentinel-fed
		// ClusterSlots callback, which suppresses CLUSTER NODES / CLUSTER
		// SLOTS / READONLY emissions to the non-cluster Redis nodes (see
		// osscluster.go: readOnly is forced false when ClusterSlots != nil).
		client := redis.NewFailoverClusterClient(re.failoverOptions(dial, tlsCfg))
		re.writeClient = client
		re.readClient = client
	case len(re.readEndpoints) > 0:
		// Explicit-replicas mode: master at re.addr, reads from re.readEndpoints.
		re.writeClient = redis.NewClient(re.clientOptions(re.addr, dial, tlsCfg))
		if len(re.readEndpoints) == 1 {
			re.readClient = redis.NewClient(re.clientOptions(re.readEndpoints[0], dial, tlsCfg))
		} else {
			// ≥2 read endpoints → readReplicaPool (random pick per GET).
			clients := make([]*redis.Client, len(re.readEndpoints))
			for i, ep := range re.readEndpoints {
				clients[i] = redis.NewClient(re.clientOptions(ep, dial, tlsCfg))
			}
			re.readPool = &readReplicaPool{clients: clients}
		}
	default:
		// Standalone — single client for both reads and writes.
		client := redis.NewClient(re.clientOptions(re.addr, dial, tlsCfg))
		re.writeClient = client
		re.readClient = client
	}

	return nil
}

// verifyConnectivity pings the write client (and the single read replica, if
// distinct) to surface an unreachable Redis at startup. A failure here is
// transient, not fatal: the clients are live and go-redis redials on the next
// command, so the caller logs and continues rather than aborting CoreDNS.
// Must be called only after a successful buildClients (writeClient non-nil).
func (re *Redis) verifyConnectivity() error {
	ctx := context.Background()

	if err := re.writeClient.Ping(ctx).Err(); err != nil {
		return err
	}
	if re.readClient != nil && re.readClient != re.writeClient {
		// Reached only in single-read-replica mode, where the address is
		// known explicitly (re.readEndpoints[0]).
		readAddr := re.readEndpoints[0]
		if err := re.readClient.Ping(ctx).Err(); err != nil {
			log.Warningf("Read endpoint %s ping failed (will retry on demand): %s", readAddr, err)
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
// Serialization is the caller's responsibility (done in WriteMsg before this
// is invoked) so pack errors and Redis-side errors stay on distinct metrics.
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
//   - (msg, storedDO, nil)  on a cache hit
//   - (nil, false, nil)     on a cache miss (key not present in Redis)
//   - (nil, false, err)     on a read error (network, timeout, protocol)
func (re *Redis) Get(ctx context.Context, key string) (*dns.Msg, bool, error) {
	// Bound the whole read path (pool wait + write + read + retries) by a
	// single readTimeout. Without this, a stalled Redis can chain
	// pool-wait + retries × per-command timeouts and stretch a DNS reply
	// well past what the README promises.
	ctx, cancel := context.WithTimeout(ctx, re.readTimeout)
	defer cancel()

	// Pipeline GET and TTL in a single round-trip. We deliberately ignore
	// the aggregate pipe.Exec error and inspect each command separately:
	// if GET succeeded but TTL failed (server hiccup, mid-pipeline I/O
	// blip, cluster-edge MOVED-on-TTL), we still have valid bytes and
	// should serve them with a fallback TTL=0 instead of throwing away
	// a perfectly good cache hit.
	pipe := re.readPipeline()
	getCmd := pipe.Get(ctx, key)
	ttlCmd := pipe.TTL(ctx, key)
	_, _ = pipe.Exec(ctx)

	b, err := getCmd.Bytes()
	if err == redis.Nil {
		return nil, false, nil // miss
	}
	if err != nil {
		return nil, false, err
	}
	if len(b) == 0 {
		// Distinct from redis.Nil: the key exists but holds an empty string
		// (RESP returns a valid zero-length bulk). Shouldn't happen for our
		// own writes, but if something put an empty value under our key,
		// evict so we don't keep handling it as a miss until natural TTL.
		re.evictAsync(ctx, key)
		return nil, false, nil
	}

	var ttl uint32
	if d, terr := ttlCmd.Result(); terr == nil && d > 0 {
		// time.Duration.Seconds() is float64; uint32 covers ~136 years, well
		// beyond any plausible DNS TTL, so the truncating cast is safe.
		ttl = uint32(d.Seconds())
	} else if terr != nil && terr != redis.Nil {
		log.Debugf("TTL fetch for %s failed (serving with ttl=0): %s", key, terr)
	}

	m, storedDO, err := FromBytes(b, ttl)
	if err != nil {
		// Corrupt wire bytes in Redis — self-heal so subsequent reads
		// don't keep tripping the same decode error.
		re.evictAsync(ctx, key)
		return nil, false, err
	}
	return m, storedDO, nil
}

// evictAsync schedules a non-blocking EXPIRE 0 (detached ctx, not DEL —
// keeps Redis's main thread free) on a detected-broken entry. Without it,
// every hit on the orphan re-trips the same bad read and bumps its error
// counter — one bad key would read as a fleet-wide incident in monitoring.
//
// Concurrent calls for the same key collapse via evictFlight so a hot
// poisoned key produces one EXPIRE, not one per hit until the first lands.
// recover() guards against a panic in the redis client crashing CoreDNS.
func (re *Redis) evictAsync(parent context.Context, key string) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("panic in evictAsync for %s: %v", key, r)
			}
		}()
		_, _, _ = re.evictFlight.Do(key, func() (interface{}, error) {
			if err := re.writeClient.Expire(context.WithoutCancel(parent), key, 0).Err(); err != nil {
				log.Debugf("Failed to evict cache key %s: %s", key, err)
			}
			return nil, nil
		})
	}()
}

// get looks up a cached response for the given request. After fetching, every
// component of the cache key — the question (qname, qtype, qclass), the DO bit,
// and the CD bit — is re-verified against the cached message as a cheap backstop
// for the (vanishingly rare) case of a 64-bit hash collision; a mismatch is
// reported via cacheCollisions and treated as a miss.
//
// The DO check works because the write path keys an entry on its RESPONSE's DO
// bit and stores that same response, so a stored entry's DO always equals its
// slot's DO. A DO=0 request therefore only ever finds DO=0 entries; a DO=1 reply
// to a DO=0 query lives in the DO=1 slot and is correctly not served to DO=0
// clients (RFC 4035 §3.2.1). See WriteMsg for the write-side decision.
func (re *Redis) get(ctx context.Context, state request.Request, server string) *dns.Msg {
	start := time.Now()
	defer func() { cacheRequests.WithLabelValues(server).Observe(time.Since(start).Seconds()) }()

	k := re.keyer.key(state.Name(), state.QClass(), state.QType(), state.Do(), state.Req.CheckingDisabled)

	m, cachedDO, err := re.Get(ctx, k)
	if err != nil {
		// Counter increments unconditionally; the log line is throttled to
		// avoid drowning log pipelines during a Redis outage.
		cacheReadErrors.WithLabelValues(server, errReason(err)).Inc()
		re.readErrLog.Do(func() {
			log.Warningf("Redis cache read error for %s: %s", state.Name(), err)
		})
		return nil
	}
	if m == nil {
		return nil
	}
	if !state.Match(m) || m.Question[0].Qclass != state.QClass() || cachedDO != state.Do() || m.CheckingDisabled != state.Req.CheckingDisabled {
		log.Warningf("Redis cache returned mismatched question (got name=%q type=%d class=%d do=%t cd=%t, want name=%q type=%d class=%d do=%t cd=%t)",
			m.Question[0].Name, m.Question[0].Qtype, m.Question[0].Qclass, cachedDO, m.CheckingDisabled,
			state.Name(), state.QType(), state.QClass(), state.Do(), state.Req.CheckingDisabled)
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

// errReason categorises a Redis client error for the reason= label on
// cache_{get,set}_errors_total. The three buckets are operationally distinct:
//
//   - "timeout"    — context deadline / cancellation, net.Error with
//     Timeout()==true, or redis.ErrPoolTimeout (pool wait
//     exceeded). Look at Redis latency, CPU, pool sizing.
//   - "connection" — non-timeout net errors and raw end-of-stream:
//     dial refused, reset, io.EOF / io.ErrUnexpectedEOF from
//     a half-closed peer. Look at connectivity, DNS, firewall,
//     Redis liveness.
//   - "other"      — RESP-level (NOAUTH, WRONGPASS, WRONGTYPE, parse errors,
//     unhandled MOVED, etc.) or anything not net-related.
//     Typically a config or code issue rather than a transient
//     outage.
//
// io.EOF is checked explicitly because go-redis's RESP reader surfaces a
// server-side connection close as a raw io.EOF (not wrapped in *net.OpError),
// which would otherwise fall through to "other" and hide a real connectivity
// incident behind the application-error bucket. TestErrReason_RealClientErrors
// pins each bucket against the live go-redis client so a future upgrade that
// reshapes errors fails loudly here rather than silently mis-bucketing.
//
// Callers must not pass redis.Nil here — that is a cache miss, not an error,
// and is filtered out earlier on the read path.
func errReason(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, redis.ErrPoolTimeout) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return "timeout"
		}
		return "connection"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "connection"
	}
	return "other"
}
