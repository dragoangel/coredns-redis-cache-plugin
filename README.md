# redis_cache

## Name

*redis_cache* — shared L2 DNS cache backed by a Redis-compatible key-value store.

## Description

*redis_cache* stores DNS responses in a shared Redis-compatible backend (Redis, Valkey, or any
RESP-protocol server) so that multiple CoreDNS instances can amortize upstream lookups across
the fleet — e.g. several pods in a Kubernetes cluster, or a fleet of node-local-dns daemons.
It is intended to sit *behind* the built-in *cache* plugin, which stays as the L1 (in-process)
cache; *redis_cache* is the L2 (networked) cache.

If the Redis backend is unreachable the plugin becomes a noop and lookups continue to flow
through the rest of the chain — DNS resolution is never blocked on the cache.

Each response is cached for the duration of its record TTL, clamped into a configurable range:
`max(min, min(record_TTL, max))`. Defaults are `1h` max for positive responses and `30m` max
for denials, both with no minimum floor; raise or lower either bound via the `success` and
`denial` directives.

Spiritual successor to [miekg/redis](https://github.com/miekg/redis) (directive `redisc`,
archived November 2025).

## Syntax

```txt
redis_cache [ZONES...] {
    endpoint ENDPOINT
    read_endpoint ENDPOINT [ENDPOINT...]
    db NUMBER
    sentinel MASTER_NAME SENTINEL_ADDR [SENTINEL_ADDR...]
    cluster SEED_ADDR [SEED_ADDR...]
    read_from latency|random|primary
    username USERNAME
    password PASSWORD
    sentinel_username USERNAME
    sentinel_password PASSWORD
    success MAX_TTL [MIN_TTL]
    denial MAX_TTL [MIN_TTL]
    timeout {
        connect DURATION
        read DURATION
        write DURATION
    }
    pool {
        size N
        min_idle N
        max_idle N
        max_active N
        max_idle_time DURATION
        max_lifetime DURATION
        wait_timeout DURATION
    }
    retries {
        max N
        min_backoff DURATION
        max_backoff DURATION
    }
    tcp_keepalive DURATION
    tls
    tls_cert PATH
    tls_key PATH
    tls_ca PATH
    tls_verify_chain BOOL
    tls_verify_hostname BOOL
    resolver ADDRESS
}
```

Each sub-directive can be omitted; when present, its own arguments are required as documented
below. Bare `redis_cache` with no block attempts to connect to `127.0.0.1:6379` with default
TTL bounds — useful only against a sidecar Redis on localhost; production deployments must
specify at least one of `endpoint`, `sentinel`, or `cluster`. The chosen topology mode
determines which other directives are valid; the parser errors at load time on conflicting
combinations rather than silently ignoring them:

* `cluster` mode (the `cluster` directive is set) rejects `endpoint`, `read_endpoint`,
  `sentinel`, and any `db` other than `0` (Redis Cluster only supports DB 0). Seed addresses
  come from `cluster`; the rest of the topology is discovered via `CLUSTER SLOTS`.
* `sentinel` mode (the `sentinel` directive is set) rejects `endpoint` and `read_endpoint` —
  the master and replicas are discovered via Sentinel.
* Default mode (neither `cluster` nor `sentinel`): writes go to `endpoint`. If no
  `read_endpoint` is given, the same client handles reads (standalone). If one or more
  `read_endpoint` entries are listed, reads are routed across those replicas instead.
  Rejects `read_from` and `sentinel_username` / `sentinel_password`.

* **ZONES** (positional) — zones to cache for. Defaults to the surrounding server-block zones.
* `endpoint` — write endpoint address (default `127.0.0.1:6379`). Accepts IPs or hostnames.
  If a port is omitted, 6379 is assumed.
* `read_endpoint` — one or more read-only replica addresses. When specified, GET operations are
  routed to these replicas while SET operations go to `endpoint`.
* `db NUMBER` — Redis logical database index for the data plane. Default `0`. Not allowed in
  `cluster` mode (Redis Cluster supports only DB 0).
* `sentinel` — enable Sentinel mode. **Master Group Name is mandatory** and must be followed
  by one or more sentinel addresses. When configured, the plugin discovers the current master
  and replicas automatically. Writes route to the master; reads route to replicas.
* `cluster` — enable Cluster mode. Takes one or more seed node addresses; the smart client
  discovers the full topology via `CLUSTER SLOTS`. Mutually exclusive with `sentinel` and
  `read_endpoint`. The `endpoint` directive is ignored in cluster mode.
* `read_from` — replica routing strategy in cluster mode. Only valid when `cluster` is set.
    * `latency` (default) — pick the replica with the lowest measured RTT.
    * `random` — pick a random replica.
    * `primary` — read only from primaries (no replica reads).
* `username` — ACL username for the data plane (primary, replicas, or cluster nodes). Optional.
* `password` — AUTH password for the data plane. Optional.
* `sentinel_username` — ACL username for the Sentinel API. Optional; only used in `sentinel` mode.
* `sentinel_password` — AUTH password for the Sentinel API. Optional; only used in `sentinel` mode.
* `success MAX_TTL [MIN_TTL]` — override TTL bounds for positive responses. MAX_TTL caps
  the cache duration (default `1h`). MIN_TTL sets a floor (default `0`) — when the upstream
  record TTL is shorter than this value, the cache duration is raised to this floor. Each
  value accepts a Go duration (`30s`, `1h`) or a bare integer (seconds); sub-second values
  like `500ms` are rejected.
* `denial MAX_TTL [MIN_TTL]` — same as `success` but for negative responses (NXDOMAIN/NODATA).
  Defaults: MAX_TTL `30m`, MIN_TTL `0`.
* `timeout` — Redis connection and operation timeouts:
    * `connect` — TCP dial timeout (default: `1s`).
    * `read` — per-command read timeout (default: `1s`).
    * `write` — per-command write timeout (default: `2s`).
* `pool` — connection-pool tuning. Each setting maps directly to the corresponding go-redis
  field; any value left unset (i.e. directive omitted) falls through to go-redis's documented
  default — values noted below in parentheses are those upstream defaults, *not* hardcoded
  here. All values are non-negative integers.
    * `size N` — maximum sockets per client (default `10 × runtime.GOMAXPROCS()`).
    * `min_idle N` — minimum idle sockets to keep warm (default `0`).
    * `max_idle N` — maximum idle sockets (default `0` = unlimited).
    * `max_active N` — hard cap on total open sockets including in-use (default `0` =
      unlimited).
    * `max_idle_time DURATION` — close a connection that has been idle for this long
      (default `30m`). Set to less than your load balancer / NAT idle drop window.
    * `max_lifetime DURATION` — force-recycle any connection older than this regardless
      of activity (default `0` = no limit).
    * `wait_timeout DURATION` — how long a query waits for a free connection when the
      pool is saturated before erroring (default: go-redis uses `read_timeout + 1s`).
* `retries` — retry behavior for transient network errors:
    * `max N` — number of retries per operation. **Plugin default is `1`** (one retry on
      transient errors; absorbs an isolated dropped packet without amplifying a sustained
      outage into multi-second DNS waits). Set `-1` to disable retries entirely; `0` falls
      back to go-redis's default of `3` (rarely what you want for an L2 DNS cache);
      positive values are taken literally.
    * `min_backoff DURATION` — initial backoff between retries (default `8ms` — go-redis).
    * `max_backoff DURATION` — cap on backoff between retries (default `512ms` — go-redis).
    Constraint: `min_backoff` must not exceed `max_backoff` when both are set.
* `tcp_keepalive DURATION` — interval for TCP keepalive probes on Redis connections.
  Default uses Go's built-in keepalive interval. Set to a value smaller than your firewall
  / NAT / service-mesh idle-drop window (often `60s`–`5m`) to keep long-lived idle
  connections from being silently killed.
* `tls` — enable TLS. **No args.** Verifies the server cert against the OS trust store.
  Use `tls_ca` to override the trust store, `tls_cert`/`tls_key` for mTLS. Implicitly
  enabled by any other `tls_*` directive — bare `tls` is only needed when no other TLS
  knob is set. The TLS config applies to every connection the plugin opens (Sentinel API,
  master, replicas, cluster nodes) — bundle CAs if planes use different roots.
* `tls_cert PATH` — PEM client certificate for mTLS. Must be paired with `tls_key`.
* `tls_key PATH` — PEM private key matching `tls_cert`.
* `tls_ca PATH` — PEM CA file used to verify the server certificate. **Replaces** the OS
  trust store when set; use only when your server's cert chains to a CA the OS doesn't ship.
* `tls_verify_chain BOOL` — verify the server certificate chains to a trusted root.
  Default `on`. Set to `off` to disable all server-cert verification (chain *and* hostname);
  use only for development or fully-trusted networks. Accepts `on`/`off`, `true`/`false`,
  `yes`/`no`, `1`/`0`.
* `tls_verify_hostname BOOL` — verify the server cert's SAN/CN matches the hostname dialed.
  Default `on`. Set to `off` when one configuration connects to multiple peers (cluster
  seeds, sentinel quorum, replication followers) whose certs share a CA but each carry
  their own hostname — chain verification still runs, only the per-host SAN/CN check is
  skipped. Has no effect when `tls_verify_chain` is `off` (chain-off implies hostname-off).
* `resolver ADDRESS` — DNS server to use for resolving Redis endpoint hostnames instead of the
  system resolver. Useful in deployments where CoreDNS itself intercepts the system resolver
  (e.g. node-local-dns) and resolving the Redis service name through it would create a circular
  dependency. Set this to an upstream DNS service IP. Port defaults to 53.

#### Authentication

The data plane (Redis nodes) and the Sentinel API authenticate independently — credentials
across the two planes may be the same or different. In each plane the auth mode follows the
standard Redis convention:

* neither set → unauthenticated.
* password only → legacy `AUTH <password>` (matches `requirepass` on any version, or
  authenticates as the `default` user on ACL-enabled servers).
* username + password → full ACL auth (Redis 6+ for the data plane, Sentinel 6.2+ for the
  Sentinel API).

## Known Compatibility

The plugin speaks only standard RESP commands (`AUTH`, `GET`, `SET … EX`, `TTL`, `PING`,
plus `CLUSTER SLOTS` in cluster mode and `SENTINEL get-master-addr-by-name` in Sentinel mode),
so it is expected to work with any reasonably complete Redis-protocol implementation. The
tables below list what's been verified versus what's expected to work based on each engine's
documented protocol coverage.

### Tested

| Server      | Standalone / Replicas | Sentinel | Cluster |
|-------------|:---------------------:|:--------:|:-------:|
| Redis 6.x   | ✅                    | ✅       | ✅      |
| Redis 7.x   | ✅                    | ✅       | ✅      |
| Redis 8.x   | ✅                    | ✅       | ✅      |
| Valkey 7.x  | ✅                    | ✅       | ✅      |
| Valkey 8.x  | ✅                    | ✅       | ✅      |
| Valkey 9.x  | ✅                    | ✅       | ✅      |

Hosted services that simply run Redis or Valkey (AWS ElastiCache, Google Memorystore, Azure
Cache for Redis, Redis Cloud, Aiven, DigitalOcean, Render, Heroku, etc.) are covered by the
rows above — connect to the service's primary endpoint in standalone mode, or to its cluster
configuration endpoint in cluster mode.

### Other RESP-compatible engines (expected)

These are alternative implementations of the Redis protocol. None are part of the tested
matrix; status is inferred from each project's documented protocol coverage.

| Engine        | Standalone / Replicas | Sentinel | Cluster | Notes |
|---------------|:---------------------:|:--------:|:-------:|-------|
| Tair          | ✅                    | ✅       | ✅      | Alibaba's Redis-derived engine. |
| Redict        | ✅                    | ✅       | ✅      | LGPL hard fork of Redis 7.2.4 — same code paths. |
| KeyDB         | ✅                    | ✅       | ✅      | Multi-threaded Redis fork; full Sentinel + Cluster support. |
| KVRocks       | ✅                    | ⚠️       | ✅      | Apache project, RocksDB-backed. Cluster mode native; Sentinel works via an external sentinel monitoring the RESP endpoint. |
| Garnet        | ✅                    | ❌       | ✅      | Microsoft's .NET Redis-compatible server; implements RESP + Redis Cluster but not Sentinel. |
| DiceDB        | ✅                    | ❌       | ❌      | Single-node async-focused reimplementation. |
| DragonflyDB   | ✅                    | ❌       | ❌      | Multi-threaded RESP server. Uses replication only — no Sentinel, no Redis-Cluster protocol. Use `endpoint` (and optionally `read_endpoint`). |
| AWS MemoryDB  | ❌                    | ❌       | ✅      | Custom durable engine that speaks the Redis protocol; not Redis OSS. Cluster-only — there is no non-cluster deployment mode. Connect via the cluster endpoint with `cluster`. |

> Reports of working / non-working combinations from real deployments are welcome via issues.

## Building

*redis_cache* is an external CoreDNS plugin and must be compiled into the CoreDNS binary.
See [Compile-time enabling or disabling plugins](https://coredns.io/2017/07/25/compile-time-enabling-or-disabling-plugins/)
for the general mechanism.

In a checkout of [coredns/coredns](https://github.com/coredns/coredns), add this line to
`plugin.cfg`. **It must appear after the `cache:cache` line** — the in-process `cache` runs
as L1 and `redis_cache` as L2:

```text
cache:cache
redis_cache:github.com/dragoangel/coredns-redis-cache-plugin
```

Then build:

```sh
go generate
go build
```

The resulting `coredns` binary now recognizes the `redis_cache` directive in your Corefile.

## Development

### First-time setup

You need the Go toolchain (version per `go.mod`), `golangci-lint`, and the
[`pre-commit`](https://pre-commit.com) Python tool.

**Linux/macOS:**

```sh
make tools                  # installs golangci-lint at the pinned version
pip install --user pre-commit
make hooks                  # runs `pre-commit install`
```

**Windows** (winget is built into Windows 10/11):

```cmd
winget install GoLang.Go
winget install GolangCI.golangci-lint
winget install Python.Python.3

:: pre-commit has no first-party winget package; install via pip:
pip install --user pre-commit

:: register the hooks
pre-commit install
```

After installing tools via winget, **close and reopen the terminal** so the
updated `PATH` is picked up. If `pre-commit` is still not found, add
`%APPDATA%\Python\Python3XX\Scripts` (replace `3XX` with your installed
version, see `where python`) to your user `PATH`.

From this point every `git commit` runs `gofmt`/`goimports`, `go vet`,
`go mod tidy`, the test suite, and `golangci-lint`. The hooks invoke `go`
and `golangci-lint` directly, so they work on Linux, macOS, and native
Windows without `make` or a POSIX shell.

### Day-to-day

The `Makefile` wraps the canonical commands for Linux/macOS users. Run
`make help` for the full list. Common targets:

```sh
make test          # go test ./...
make test-race     # go test -race ./...
make lint          # golangci-lint run
make fmt           # gofmt -s -w .
make ci            # full pipeline: fmt-check + tidy-check + vet + lint + test-race
```

Windows users can call the underlying commands directly (`go test ./...`,
`golangci-lint run ./...`, `go vet ./...`, …) or run
`pre-commit run --all-files` for the same pipeline pre-commit enforces on
commit.

### Automation

CI (`.github/workflows/ci.yml`) runs the same checks on push and PR.
Dependency bumps are tracked by Dependabot (`gomod` + `github-actions`,
weekly, grouped). Pre-commit hook pins are bumped via
`pre-commit autoupdate` or [pre-commit.ci](https://pre-commit.ci).

## Metrics

If monitoring is enabled (via the *prometheus* directive) then the following metrics are exported:

* `coredns_redis_cache_hits_total{server}` — The count of cache hits from Redis.
* `coredns_redis_cache_misses_total{server}` — The count of cache misses from Redis.
* `coredns_redis_cache_get_errors_total{server}` — The count of errors when reading entries from Redis.
* `coredns_redis_cache_set_errors_total{server}` — The count of errors when adding entries to Redis.
* `coredns_redis_cache_encode_errors_total{server}` — The count of DNS messages that could not be serialized to wire format and so were not cached.
* `coredns_redis_cache_response_mismatches_total{server}` — The count of upstream replies whose question did not match the original request and were therefore refused for caching (the reply itself is still passed to the client). Non-zero suggests a misbehaving forwarder upstream or an attempted cache-poisoning probe.
* `coredns_redis_cache_collisions_total{server}` — The count of cache hits whose stored question did not match the request (treated as a miss; non-zero indicates corruption, version skew, or — extremely unlikely — a 64-bit key hash collision).

## Examples

Examples after the first show only the `redis_cache { ... }` block; wrap it in the same
`. { cache {...} … forward . … }` shape from the Standalone example. They also omit
`success` / `denial` — reuse the values from Standalone or rely on the defaults documented
in the directive list.

### Standalone (full Corefile)

```corefile
. {
    cache {
        success 9984 30
        denial 9984 5
    }
    redis_cache {
        endpoint redis.cache.svc.cluster.local:6379
        success 1h 1m
        denial 30m 30s
    }
    forward . 8.8.8.8:53
}
```

### Explicit read replicas

Writes to a known master, reads round-robin across replicas:

```corefile
redis_cache {
    endpoint 10.0.0.1:6379
    read_endpoint 10.0.0.2:6379 10.0.0.3:6379
    password secretPass
}
```

### Sentinel with read/write separation

Master Group Name is mandatory; data-plane and Sentinel-API passwords are independent.

```corefile
redis_cache {
    sentinel mymaster 10.0.0.1:26379 10.0.0.2:26379 10.0.0.3:26379
    password masterReplicaPass
    sentinel_password sentinelPass
}
```

### Redis 6+ ACL (username + password)

```corefile
redis_cache {
    endpoint redis.cache.svc.cluster.local:6379
    username dns-cache
    password s3cret
}
```

### Cluster

```corefile
redis_cache {
    cluster valkey-cluster-0:6379 valkey-cluster-1:6379 valkey-cluster-2:6379
    password secretPass
    read_from latency
}
```

> **Kubernetes note:** the smart client connects directly to every primary and replica the
> seeds advertise via `CLUSTER SLOTS`. If nodes advertise pod IPs (chart default), ensure
> they're routable from CoreDNS pods, or set `cluster-announce-hostname` on each node so the
> announced addresses match what `resolver` resolves.

### TLS — server-only

OS trust store, no client cert:

```corefile
redis_cache {
    endpoint redis.example.com:6380
    tls
    password s3cret
}
```

Internal CA, no client cert:

```corefile
redis_cache {
    endpoint redis.example.com:6380
    tls_ca /etc/ssl/certs/redis-ca.pem
    password s3cret
}
```

### TLS — mTLS

```corefile
redis_cache {
    endpoint redis.cache.svc.cluster.local:6379
    username dns-cache
    password s3cret
    tls_cert /etc/redis/tls/client.crt
    tls_key  /etc/redis/tls/client.key
    tls_ca   /etc/redis/tls/ca.pem
}
```

### TLS — multi-host with shared CA

Cluster / sentinel / multi-replica setups where peers share a CA but each carries its own
hostname and cert SAN not align — keep chain verification, but skip per-host SAN/CN check:

```corefile
redis_cache {
    cluster valkey-0:6379 valkey-1:6379 valkey-2:6379
    tls_ca /etc/redis/tls/ca.pem
    tls_verify_hostname off
    password s3cret
}
```

### Kubernetes node-local-dns

When CoreDNS itself intercepts the cluster DNS VIP, resolving the Redis service name through
it would loop. Use `resolver` to point at the upstream kube-dns; `__PILLAR__CLUSTER__DNS__`
is substituted by node-local-dns at runtime:

```corefile
.:53 {
    errors
    cache {
        success 9984 30
        denial 9984 5
    }
    redis_cache {
        endpoint k8s-dns-cache-redis-master.k8s-dns-cache.svc.cluster.local:6379
        read_endpoint k8s-dns-cache-redis-replicas.k8s-dns-cache.svc.cluster.local:6379
        password secretPass
        success 1h 1m
        denial 30m 30s
        resolver __PILLAR__CLUSTER__DNS__
    }
    forward . __PILLAR__UPSTREAM__SERVERS__
}
```
