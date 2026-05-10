package redis_cache

import (
	"testing"
	"time"

	"github.com/coredns/caddy"
)

func TestSetup(t *testing.T) {
	const defEndpoint = "127.0.0.1:6379"

	tests := []struct {
		input            string
		shouldErr        bool
		expectedNMaxTTL  time.Duration
		expectedPMaxTTL  time.Duration
		expectedPMinTTL  time.Duration
		expectedNMinTTL  time.Duration
		expectedEndpoint string
		expectedPassword string
		expectedMaster   string
		sentinelCount    int
		readEpCount      int
		clusterCount     int
		expectedReadFrom string
	}{
		// Basic config
		{`redis_cache`, false, maxNTTL, maxTTL, 0, 0, defEndpoint, "", "", 0, 0, 0, ""},

		// Success max only
		{`redis_cache example.nl {
			success 10
		}`, false, maxNTTL, 10 * time.Second, 0, 0, defEndpoint, "", "", 0, 0, 0, ""},

		// Success + denial max only
		{`redis_cache example.nl {
			success 10
			denial 15
		}`, false, 15 * time.Second, 10 * time.Second, 0, 0, defEndpoint, "", "", 0, 0, 0, ""},

		// Success max + min
		{`redis_cache {
			success 3600 30
			denial 1800 15
		}`, false, 1800 * time.Second, 3600 * time.Second, 30 * time.Second, 15 * time.Second, defEndpoint, "", "", 0, 0, 0, ""},

		// Only min via second arg, default max
		{`redis_cache {
			success 3600 60
		}`, false, maxNTTL, 3600 * time.Second, 60 * time.Second, 0, defEndpoint, "", "", 0, 0, 0, ""},

		// Custom endpoint with port
		{`redis_cache {
			endpoint 127.0.0.2:6379
		}`, false, maxNTTL, maxTTL, 0, 0, "127.0.0.2:6379", "", "", 0, 0, 0, ""},

		// Custom endpoint without port (defaults to 6379)
		{`redis_cache {
			endpoint 127.0.0.3
		}`, false, maxNTTL, maxTTL, 0, 0, "127.0.0.3:6379", "", "", 0, 0, 0, ""},

		// Password
		{`redis_cache {
			password secret123
		}`, false, maxNTTL, maxTTL, 0, 0, defEndpoint, "secret123", "", 0, 0, 0, ""},

		// Sentinel config
		{`redis_cache {
			sentinel mymaster 10.0.0.1:26379 10.0.0.2:26379 10.0.0.3:26379
		}`, false, maxNTTL, maxTTL, 0, 0, defEndpoint, "", "mymaster", 3, 0, 0, ""},

		// Read endpoints
		{`redis_cache {
			endpoint 10.0.0.1:6379
			read_endpoint 10.0.0.2:6379 10.0.0.3:6379
		}`, false, maxNTTL, maxTTL, 0, 0, "10.0.0.1:6379", "", "", 0, 2, 0, ""},

		// Sentinel with password
		{`redis_cache {
			sentinel mymaster 10.0.0.1:26379
			password secretPass
		}`, false, maxNTTL, maxTTL, 0, 0, defEndpoint, "secretPass", "mymaster", 1, 0, 0, ""},

		// Error: sub-second success / denial TTL is rejected
		{`redis_cache { success 500ms }`, true, 0, 0, 0, 0, "", "", "", 0, 0, 0, ""},
		{`redis_cache { success 1h 500ms }`, true, 0, 0, 0, 0, "", "", "", 0, 0, 0, ""},
		{`redis_cache { denial 1500ms }`, true, 0, 0, 0, 0, "", "", "", 0, 0, 0, ""},

		// Duration strings accepted when whole-second
		{`redis_cache {
			success 1h 1m
			denial 30m 30s
		}`, false, 30 * time.Minute, time.Hour, time.Minute, 30 * time.Second, defEndpoint, "", "", 0, 0, 0, ""},

		// Error: invalid denial TTL
		{`redis_cache example.nl {
			success 15
			denial aaa
		}`, true, 0, 0, 0, 0, "", "", "", 0, 0, 0, ""},

		// Error: unknown directive
		{`redis_cache example.nl {
			positive 15
			negative aaa
		}`, true, 0, 0, 0, 0, "", "", "", 0, 0, 0, ""},

		// Error: invalid endpoint
		{`redis_cache {
			endpoint :1:1:6379
		}`, true, 0, 0, 0, 0, "", "", "", 0, 0, 0, ""},

		// Hostname endpoint (Docker/K8s service name)
		{`redis_cache {
			endpoint valkey-master:6379
		}`, false, maxNTTL, maxTTL, 0, 0, "valkey-master:6379", "", "", 0, 0, 0, ""},

		// Hostname endpoint without port
		{`redis_cache {
			endpoint valkey-master
		}`, false, maxNTTL, maxTTL, 0, 0, "valkey-master:6379", "", "", 0, 0, 0, ""},

		// Hostname read endpoints
		{`redis_cache {
			endpoint valkey-master:6379
			read_endpoint valkey-replica-0:6379 valkey-replica-1:6379
		}`, false, maxNTTL, maxTTL, 0, 0, "valkey-master:6379", "", "", 0, 2, 0, ""},

		// Error: sentinel without addresses
		{`redis_cache {
			sentinel mymaster
		}`, true, 0, 0, 0, 0, "", "", "", 0, 0, 0, ""},

		// Error: negative success min
		{`redis_cache {
			success 3600 -5
		}`, true, 0, 0, 0, 0, "", "", "", 0, 0, 0, ""},

		// Error: negative denial min
		{`redis_cache {
			denial 1800 -10
		}`, true, 0, 0, 0, 0, "", "", "", 0, 0, 0, ""},

		// min of 0 is valid (means no floor)
		{`redis_cache {
			success 3600 0
		}`, false, maxNTTL, 3600 * time.Second, 0, 0, defEndpoint, "", "", 0, 0, 0, ""},

		// timeout block (requires unit suffix on every value)
		{`redis_cache {
			timeout {
				connect 500ms
				read 200ms
				write 1s
			}
		}`, false, maxNTTL, maxTTL, 0, 0, defEndpoint, "", "", 0, 0, 0, ""},

		// resolver
		{`redis_cache {
			resolver 10.96.0.10
		}`, false, maxNTTL, maxTTL, 0, 0, defEndpoint, "", "", 0, 0, 0, ""},

		// resolver with port
		{`redis_cache {
			resolver 10.96.0.10:5353
		}`, false, maxNTTL, maxTTL, 0, 0, defEndpoint, "", "", 0, 0, 0, ""},

		// Cluster: single seed
		{`redis_cache {
			cluster 10.0.0.1:6379
		}`, false, maxNTTL, maxTTL, 0, 0, defEndpoint, "", "", 0, 0, 1, ""},

		// Cluster: multiple seeds + read_from latency
		{`redis_cache {
			cluster 10.0.0.1:6379 10.0.0.2:6379 10.0.0.3:6379
			read_from latency
		}`, false, maxNTTL, maxTTL, 0, 0, defEndpoint, "", "", 0, 0, 3, "latency"},

		// Cluster: read_from random
		{`redis_cache {
			cluster valkey-cluster-0:6379 valkey-cluster-1:6379
			read_from random
		}`, false, maxNTTL, maxTTL, 0, 0, defEndpoint, "", "", 0, 0, 2, "random"},

		// Cluster: read_from primary
		{`redis_cache {
			cluster 10.0.0.1
			read_from primary
		}`, false, maxNTTL, maxTTL, 0, 0, defEndpoint, "", "", 0, 0, 1, "primary"},

		// Error: cluster without seeds
		{`redis_cache {
			cluster
		}`, true, 0, 0, 0, 0, "", "", "", 0, 0, 0, ""},

		// Error: cluster + sentinel
		{`redis_cache {
			cluster 10.0.0.1:6379
			sentinel mymaster 10.0.0.2:26379
		}`, true, 0, 0, 0, 0, "", "", "", 0, 0, 0, ""},

		// Error: cluster + read_endpoint
		{`redis_cache {
			cluster 10.0.0.1:6379
			read_endpoint 10.0.0.2:6379
		}`, true, 0, 0, 0, 0, "", "", "", 0, 0, 0, ""},

		// Error: cluster + explicit endpoint
		{`redis_cache {
			cluster 10.0.0.1:6379
			endpoint 10.0.0.5:6379
		}`, true, 0, 0, 0, 0, "", "", "", 0, 0, 0, ""},

		// Error: sentinel + explicit endpoint
		{`redis_cache {
			sentinel mymaster 10.0.0.1:26379
			endpoint 10.0.0.5:6379
		}`, true, 0, 0, 0, 0, "", "", "", 0, 0, 0, ""},

		// Error: sentinel + read_endpoint
		{`redis_cache {
			sentinel mymaster 10.0.0.1:26379
			read_endpoint 10.0.0.2:6379
		}`, true, 0, 0, 0, 0, "", "", "", 0, 0, 0, ""},

		// Error: sentinel_password without sentinel mode
		{`redis_cache {
			endpoint 10.0.0.1:6379
			sentinel_password sp
		}`, true, 0, 0, 0, 0, "", "", "", 0, 0, 0, ""},

		// Error: sentinel_username without sentinel mode
		{`redis_cache {
			endpoint 10.0.0.1:6379
			sentinel_username su
		}`, true, 0, 0, 0, 0, "", "", "", 0, 0, 0, ""},

		// Error: read_from without cluster
		{`redis_cache {
			read_from latency
		}`, true, 0, 0, 0, 0, "", "", "", 0, 0, 0, ""},

		// Error: invalid read_from value
		{`redis_cache {
			cluster 10.0.0.1:6379
			read_from elsewhere
		}`, true, 0, 0, 0, 0, "", "", "", 0, 0, 0, ""},

		// Error: invalid cluster seed
		{`redis_cache {
			cluster :::6379
		}`, true, 0, 0, 0, 0, "", "", "", 0, 0, 0, ""},
	}

	for i, test := range tests {
		c := caddy.NewTestController("dns", test.input)
		re, err := parse(c)
		if test.shouldErr && err == nil {
			t.Errorf("Test %d: Expected error but found nil for input: %s", i, test.input)
			continue
		} else if !test.shouldErr && err != nil {
			t.Errorf("Test %d: Expected no error but found: %v for input: %s", i, err, test.input)
			continue
		}
		if test.shouldErr {
			continue
		}

		if re.nMaxTTL != test.expectedNMaxTTL {
			t.Errorf("Test %d: Expected nttl %v but found: %v", i, test.expectedNMaxTTL, re.nMaxTTL)
		}
		if re.pMaxTTL != test.expectedPMaxTTL {
			t.Errorf("Test %d: Expected pttl %v but found: %v", i, test.expectedPMaxTTL, re.pMaxTTL)
		}
		if re.pMinTTL != test.expectedPMinTTL {
			t.Errorf("Test %d: Expected pMinTTL %v but found: %v", i, test.expectedPMinTTL, re.pMinTTL)
		}
		if re.nMinTTL != test.expectedNMinTTL {
			t.Errorf("Test %d: Expected nMinTTL %v but found: %v", i, test.expectedNMinTTL, re.nMinTTL)
		}
		if re.addr != test.expectedEndpoint {
			t.Errorf("Test %d: Expected endpoint %v but found: %v", i, test.expectedEndpoint, re.addr)
		}
		if re.password != test.expectedPassword {
			t.Errorf("Test %d: Expected password %v but found: %v", i, test.expectedPassword, re.password)
		}
		if re.masterName != test.expectedMaster {
			t.Errorf("Test %d: Expected master %v but found: %v", i, test.expectedMaster, re.masterName)
		}
		if len(re.sentinels) != test.sentinelCount {
			t.Errorf("Test %d: Expected %d sentinels but found: %d", i, test.sentinelCount, len(re.sentinels))
		}
		if len(re.readEndpoints) != test.readEpCount {
			t.Errorf("Test %d: Expected %d read endpoints but found: %d", i, test.readEpCount, len(re.readEndpoints))
		}
		if len(re.clusterAddrs) != test.clusterCount {
			t.Errorf("Test %d: Expected %d cluster seeds but found: %d", i, test.clusterCount, len(re.clusterAddrs))
		}
		if re.readFrom != test.expectedReadFrom {
			t.Errorf("Test %d: Expected readFrom %q but found: %q", i, test.expectedReadFrom, re.readFrom)
		}
	}
}

func TestSetupAuth(t *testing.T) {
	tests := []struct {
		name             string
		input            string
		shouldErr        bool
		username         string
		password         string
		sentinelUsername string
		sentinelPassword string
	}{
		{
			name: "legacy AUTH: password only, no username (default user)",
			input: `redis_cache {
				password s3cret
			}`,
			password: "s3cret",
		},
		{
			name: "ACL username + password (Redis 6+)",
			input: `redis_cache {
				username cache-user
				password s3cret
			}`,
			username: "cache-user",
			password: "s3cret",
		},
		{
			name: "no auth at all (all optional)",
			input: `redis_cache {
				endpoint 10.0.0.1:6379
			}`,
		},
		{
			name: "Sentinel with separate sentinel password (no Sentinel ACL user)",
			input: `redis_cache {
				sentinel mymaster 10.0.0.1:26379
				username app-user
				password app-pass
				sentinel_password sentinel-pass
			}`,
			username:         "app-user",
			password:         "app-pass",
			sentinelPassword: "sentinel-pass",
		},
		{
			name: "Sentinel with full ACL on both planes (Redis 6.2+)",
			input: `redis_cache {
				sentinel mymaster 10.0.0.1:26379
				username app-user
				password app-pass
				sentinel_username sentinel-user
				sentinel_password sentinel-pass
			}`,
			username:         "app-user",
			password:         "app-pass",
			sentinelUsername: "sentinel-user",
			sentinelPassword: "sentinel-pass",
		},
		{
			name: "Sentinel with no auth at all",
			input: `redis_cache {
				sentinel mymaster 10.0.0.1:26379
			}`,
		},
		{
			name: "Cluster with ACL",
			input: `redis_cache {
				cluster 10.0.0.1:6379
				username cluster-user
				password cluster-pass
			}`,
			username: "cluster-user",
			password: "cluster-pass",
		},
		{
			name: "username requires exactly one argument",
			input: `redis_cache {
				username
			}`,
			shouldErr: true,
		},
		{
			name: "sentinel_username requires exactly one argument",
			input: `redis_cache {
				sentinel_username
			}`,
			shouldErr: true,
		},
		{
			name: "sentinel_password requires exactly one argument",
			input: `redis_cache {
				sentinel_password
			}`,
			shouldErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := caddy.NewTestController("dns", tc.input)
			re, err := parse(c)
			if tc.shouldErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if re.username != tc.username {
				t.Errorf("username: got %q, want %q", re.username, tc.username)
			}
			if re.password != tc.password {
				t.Errorf("password: got %q, want %q", re.password, tc.password)
			}
			if re.sentinelUsername != tc.sentinelUsername {
				t.Errorf("sentinelUsername: got %q, want %q", re.sentinelUsername, tc.sentinelUsername)
			}
			if re.sentinelPassword != tc.sentinelPassword {
				t.Errorf("sentinelPassword: got %q, want %q", re.sentinelPassword, tc.sentinelPassword)
			}
		})
	}
}

func TestSetupTLS(t *testing.T) {
	tests := []struct {
		name              string
		input             string
		shouldErr         bool
		tlsEnabled        bool
		tlsCert           string
		tlsKey            string
		tlsCA             string
		tlsVerifyChain    bool // expected; safe defaults assume true when tlsEnabled
		tlsVerifyHostname bool
	}{
		{
			name:              "no TLS directive — disabled, defaults still safe",
			input:             `redis_cache { endpoint 10.0.0.1:6379 }`,
			tlsVerifyChain:    true,
			tlsVerifyHostname: true,
		},
		{
			name: "tls alone — OS trust store, no client auth",
			input: `redis_cache {
				tls
			}`,
			tlsEnabled:        true,
			tlsVerifyChain:    true,
			tlsVerifyHostname: true,
		},
		{
			name: "tls_ca — custom CA, no client auth",
			input: `redis_cache {
				tls_ca /etc/ssl/redis-ca.pem
			}`,
			tlsEnabled:        true,
			tlsCA:              "/etc/ssl/redis-ca.pem",
			tlsVerifyChain:    true,
			tlsVerifyHostname: true,
		},
		{
			name: "tls_cert + tls_key — mTLS with system trust",
			input: `redis_cache {
				tls_cert /etc/ssl/client.crt
				tls_key /etc/ssl/client.key
			}`,
			tlsEnabled:        true,
			tlsCert:            "/etc/ssl/client.crt",
			tlsKey:             "/etc/ssl/client.key",
			tlsVerifyChain:    true,
			tlsVerifyHostname: true,
		},
		{
			name: "tls_cert + tls_key + tls_ca — mTLS with custom CA",
			input: `redis_cache {
				tls_cert /etc/ssl/client.crt
				tls_key /etc/ssl/client.key
				tls_ca /etc/ssl/ca.pem
			}`,
			tlsEnabled:        true,
			tlsCert:            "/etc/ssl/client.crt",
			tlsKey:             "/etc/ssl/client.key",
			tlsCA:              "/etc/ssl/ca.pem",
			tlsVerifyChain:    true,
			tlsVerifyHostname: true,
		},
		{
			name: "tls_verify_hostname off — multi-host setup, trust chain only",
			input: `redis_cache {
				tls_ca /etc/ssl/ca.pem
				tls_verify_hostname off
			}`,
			tlsEnabled:        true,
			tlsCA:              "/etc/ssl/ca.pem",
			tlsVerifyChain:    true,
			tlsVerifyHostname: false,
		},
		{
			name: "tls_verify_chain off — full skip (dev only)",
			input: `redis_cache {
				tls_verify_chain off
			}`,
			tlsEnabled:        true,
			tlsVerifyChain:    false,
			tlsVerifyHostname: true, // hostname flag retains default; chain-off makes it moot
		},
		{
			name: "all TLS directives combined",
			input: `redis_cache {
				tls_cert /c.crt
				tls_key /c.key
				tls_ca /ca.pem
				tls_verify_chain on
				tls_verify_hostname off
			}`,
			tlsEnabled:        true,
			tlsCert:            "/c.crt",
			tlsKey:             "/c.key",
			tlsCA:              "/ca.pem",
			tlsVerifyChain:    true,
			tlsVerifyHostname: false,
		},
		{
			name: "tls accepts no arguments (positional form rejected)",
			input: `redis_cache {
				tls /a /b /c
			}`,
			shouldErr: true,
		},
		{
			name: "tls_cert requires exactly one argument",
			input: `redis_cache {
				tls_cert
			}`,
			shouldErr: true,
		},
		{
			name: "tls_key requires exactly one argument",
			input: `redis_cache {
				tls_key
			}`,
			shouldErr: true,
		},
		{
			name: "tls_ca requires exactly one argument",
			input: `redis_cache {
				tls_ca
			}`,
			shouldErr: true,
		},
		{
			name: "tls_verify_chain requires a boolean argument",
			input: `redis_cache {
				tls_verify_chain
			}`,
			shouldErr: true,
		},
		{
			name: "tls_verify_chain rejects non-boolean values",
			input: `redis_cache {
				tls_verify_chain maybe
			}`,
			shouldErr: true,
		},
		{
			name: "tls_verify_hostname requires a boolean argument",
			input: `redis_cache {
				tls_verify_hostname
			}`,
			shouldErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := caddy.NewTestController("dns", tc.input)
			re, err := parse(c)
			if tc.shouldErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if re.tlsEnabled != tc.tlsEnabled {
				t.Errorf("tlsEnabled: got %v, want %v", re.tlsEnabled, tc.tlsEnabled)
			}
			if re.tlsCert != tc.tlsCert {
				t.Errorf("tlsCert: got %q, want %q", re.tlsCert, tc.tlsCert)
			}
			if re.tlsKey != tc.tlsKey {
				t.Errorf("tlsKey: got %q, want %q", re.tlsKey, tc.tlsKey)
			}
			if re.tlsCA != tc.tlsCA {
				t.Errorf("tlsCA: got %q, want %q", re.tlsCA, tc.tlsCA)
			}
			if re.tlsVerifyChain != tc.tlsVerifyChain {
				t.Errorf("tlsVerifyChain: got %v, want %v", re.tlsVerifyChain, tc.tlsVerifyChain)
			}
			if re.tlsVerifyHostname != tc.tlsVerifyHostname {
				t.Errorf("tlsVerifyHostname: got %v, want %v", re.tlsVerifyHostname, tc.tlsVerifyHostname)
			}
		})
	}
}

func TestSetupPoolAndNetwork(t *testing.T) {
	tests := []struct {
		name            string
		input           string
		shouldErr       bool
		db              int
		poolSize        int
		minIdleConns    int
		maxIdleConns    int
		maxActiveConns  int
		connMaxIdleTime time.Duration
		connMaxLifetime time.Duration
		poolTimeout     time.Duration
		maxRetries      int
		minRetryBackoff time.Duration
		maxRetryBackoff time.Duration
		tcpKeepalive    time.Duration
	}{
		{
			name:       "no pool/retries/network directives — defaults: maxRetries=1, rest go-redis",
			input:      `redis_cache { endpoint 10.0.0.1:6379 }`,
			maxRetries: 1, // plugin default — see New()
		},
		{
			name: "full pool block (Go duration strings)",
			input: `redis_cache {
				pool {
					size 50
					min_idle 5
					max_idle 100
					max_active 200
					max_idle_time 4m
					max_lifetime 30m
					wait_timeout 5s
				}
			}`,
			poolSize:        50,
			minIdleConns:    5,
			maxIdleConns:    100,
			maxActiveConns:  200,
			connMaxIdleTime: 4 * time.Minute,
			connMaxLifetime: 30 * time.Minute,
			poolTimeout:     5 * time.Second,
			maxRetries:      1, // plugin default — see New()
		},
		{
			name: "pool durations require a unit suffix (no bare-int fallback)",
			input: `redis_cache {
				pool {
					max_idle_time 240
				}
			}`,
			shouldErr: true,
		},
		{
			name: "tcp_keepalive requires a unit suffix",
			input: `redis_cache {
				tcp_keepalive 30
			}`,
			shouldErr: true,
		},
		{
			name: "retries backoff requires a unit suffix",
			input: `redis_cache {
				retries {
					min_backoff 100
				}
			}`,
			shouldErr: true,
		},
		{
			name: "timeouts require a unit suffix",
			input: `redis_cache {
				timeout {
					connect 500
				}
			}`,
			shouldErr: true,
		},
		{
			name: "retries block (Go duration strings for backoffs)",
			input: `redis_cache {
				retries {
					max 5
					min_backoff 16ms
					max_backoff 1024ms
				}
			}`,
			maxRetries:      5,
			minRetryBackoff: 16 * time.Millisecond,
			maxRetryBackoff: 1024 * time.Millisecond,
		},
		{
			name: "retries max -1 disables retries",
			input: `redis_cache {
				retries {
					max -1
				}
			}`,
			maxRetries: -1,
		},
		{
			name: "tcp_keepalive top-level (Go duration string)",
			input: `redis_cache {
				tcp_keepalive 30s
			}`,
			tcpKeepalive: 30 * time.Second,
			maxRetries:   1,
		},
		{
			name: "db non-zero in standalone mode",
			input: `redis_cache {
				endpoint 10.0.0.1:6379
				db 3
			}`,
			db:         3,
			maxRetries: 1,
		},
		{
			name: "everything combined",
			input: `redis_cache {
				endpoint 10.0.0.1:6379
				db 1
				pool {
					size 32
					min_idle 4
					max_idle_time 1m
				}
				retries {
					max 5
					min_backoff 8ms
					max_backoff 256ms
				}
				tcp_keepalive 15s
			}`,
			db:              1,
			poolSize:        32,
			minIdleConns:    4,
			connMaxIdleTime: 1 * time.Minute,
			maxRetries:      5,
			minRetryBackoff: 8 * time.Millisecond,
			maxRetryBackoff: 256 * time.Millisecond,
			tcpKeepalive:    15 * time.Second,
		},

		// Errors
		{
			name: "pool size cannot be negative",
			input: `redis_cache {
				pool { size -1 }
			}`,
			shouldErr: true,
		},
		{
			name: "pool max_idle_time cannot be negative",
			input: `redis_cache {
				pool { max_idle_time -1 }
			}`,
			shouldErr: true,
		},
		{
			name: "pool unknown directive",
			input: `redis_cache {
				pool { foo 1 }
			}`,
			shouldErr: true,
		},
		{
			name: "retries min_backoff cannot be negative",
			input: `redis_cache {
				retries { min_backoff -1 }
			}`,
			shouldErr: true,
		},
		{
			name: "retries min_backoff > max_backoff is rejected",
			input: `redis_cache {
				retries {
					min_backoff 1000
					max_backoff 100
				}
			}`,
			shouldErr: true,
		},
		{
			name: "retries unknown directive",
			input: `redis_cache {
				retries { foo 1 }
			}`,
			shouldErr: true,
		},
		{
			name: "tcp_keepalive cannot be negative",
			input: `redis_cache {
				tcp_keepalive -1
			}`,
			shouldErr: true,
		},
		{
			name: "tcp_keepalive requires exactly one argument",
			input: `redis_cache {
				tcp_keepalive
			}`,
			shouldErr: true,
		},
		{
			name: "db cannot be negative",
			input: `redis_cache {
				db -1
			}`,
			shouldErr: true,
		},
		{
			name: "db != 0 with cluster is rejected",
			input: `redis_cache {
				cluster 10.0.0.1:6379
				db 5
			}`,
			shouldErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := caddy.NewTestController("dns", tc.input)
			re, err := parse(c)
			if tc.shouldErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if re.db != tc.db {
				t.Errorf("db: got %d, want %d", re.db, tc.db)
			}
			if re.poolSize != tc.poolSize {
				t.Errorf("poolSize: got %d, want %d", re.poolSize, tc.poolSize)
			}
			if re.minIdleConns != tc.minIdleConns {
				t.Errorf("minIdleConns: got %d, want %d", re.minIdleConns, tc.minIdleConns)
			}
			if re.maxIdleConns != tc.maxIdleConns {
				t.Errorf("maxIdleConns: got %d, want %d", re.maxIdleConns, tc.maxIdleConns)
			}
			if re.maxActiveConns != tc.maxActiveConns {
				t.Errorf("maxActiveConns: got %d, want %d", re.maxActiveConns, tc.maxActiveConns)
			}
			if re.connMaxIdleTime != tc.connMaxIdleTime {
				t.Errorf("connMaxIdleTime: got %v, want %v", re.connMaxIdleTime, tc.connMaxIdleTime)
			}
			if re.connMaxLifetime != tc.connMaxLifetime {
				t.Errorf("connMaxLifetime: got %v, want %v", re.connMaxLifetime, tc.connMaxLifetime)
			}
			if re.poolTimeout != tc.poolTimeout {
				t.Errorf("poolTimeout: got %v, want %v", re.poolTimeout, tc.poolTimeout)
			}
			if re.maxRetries != tc.maxRetries {
				t.Errorf("maxRetries: got %d, want %d", re.maxRetries, tc.maxRetries)
			}
			if re.minRetryBackoff != tc.minRetryBackoff {
				t.Errorf("minRetryBackoff: got %v, want %v", re.minRetryBackoff, tc.minRetryBackoff)
			}
			if re.maxRetryBackoff != tc.maxRetryBackoff {
				t.Errorf("maxRetryBackoff: got %v, want %v", re.maxRetryBackoff, tc.maxRetryBackoff)
			}
			if re.tcpKeepalive != tc.tcpKeepalive {
				t.Errorf("tcpKeepalive: got %v, want %v", re.tcpKeepalive, tc.tcpKeepalive)
			}
		})
	}
}

func TestBuildTLSConfig(t *testing.T) {
	t.Run("disabled returns nil config", func(t *testing.T) {
		re := &Redis{}
		cfg, err := re.buildTLSConfig()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Errorf("expected nil tls.Config, got %+v", cfg)
		}
	})

	t.Run("enabled with full verification: standard config, no overrides", func(t *testing.T) {
		re := &Redis{tlsEnabled: true, tlsVerifyChain: true, tlsVerifyHostname: true}
		cfg, err := re.buildTLSConfig()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg == nil {
			t.Fatal("expected non-nil tls.Config")
		}
		if cfg.InsecureSkipVerify {
			t.Error("expected InsecureSkipVerify=false for full-verification config")
		}
		if cfg.VerifyConnection != nil {
			t.Error("expected VerifyConnection nil for full-verification config")
		}
		if cfg.RootCAs != nil {
			t.Errorf("expected RootCAs nil (system trust), got %v", cfg.RootCAs)
		}
	})

	t.Run("verify_chain off: full skip (InsecureSkipVerify=true, no custom verifier)", func(t *testing.T) {
		re := &Redis{tlsEnabled: true, tlsVerifyChain: false, tlsVerifyHostname: true}
		cfg, err := re.buildTLSConfig()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !cfg.InsecureSkipVerify {
			t.Error("expected InsecureSkipVerify=true when verify_chain=off")
		}
		if cfg.VerifyConnection != nil {
			t.Error("expected VerifyConnection nil — chain-off skips everything")
		}
	})

	t.Run("verify_hostname off but chain on: custom verifier set", func(t *testing.T) {
		re := &Redis{tlsEnabled: true, tlsVerifyChain: true, tlsVerifyHostname: false}
		cfg, err := re.buildTLSConfig()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !cfg.InsecureSkipVerify {
			t.Error("expected InsecureSkipVerify=true (custom verifier handles chain)")
		}
		if cfg.VerifyConnection == nil {
			t.Error("expected VerifyConnection non-nil for chain-only verification")
		}
	})

	t.Run("client cert without key fails", func(t *testing.T) {
		re := &Redis{tlsEnabled: true, tlsCert: "/x", tlsVerifyChain: true, tlsVerifyHostname: true}
		if _, err := re.buildTLSConfig(); err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("missing CA file returns error", func(t *testing.T) {
		re := &Redis{tlsEnabled: true, tlsCA: "/nonexistent/ca.pem", tlsVerifyChain: true, tlsVerifyHostname: true}
		if _, err := re.buildTLSConfig(); err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}
