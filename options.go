package redis_cache

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/redis/go-redis/v9"
)

// dialFunc is go-redis's dialer signature, reused from re.dialer().
type dialFunc = func(ctx context.Context, network, addr string) (net.Conn, error)

// clientOptions builds the *redis.Options literal used for plain Redis clients
// (standalone master, single read replica, or per-replica clients in the
// random-LB pool). Only `Addr` varies per call site; everything else is
// pulled from the parsed plugin config.
func (re *Redis) clientOptions(addr string, dial dialFunc, tlsCfg *tls.Config) *redis.Options {
	return &redis.Options{
		Addr:            addr,
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
	}
}

// clusterOptions builds the base *redis.ClusterOptions for cluster mode.
// Caller layers ReadOnly / RouteByLatency / RouteRandomly on top based on
// the `read_from` directive.
func (re *Redis) clusterOptions(dial dialFunc, tlsCfg *tls.Config) *redis.ClusterOptions {
	return &redis.ClusterOptions{
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
}

// failoverOptions builds the *redis.FailoverOptions for Sentinel mode.
// RouteRandomly is set here because the plugin always wants random-LB across
// Sentinel-discovered replicas (see connect()).
func (re *Redis) failoverOptions(dial dialFunc, tlsCfg *tls.Config) *redis.FailoverOptions {
	return &redis.FailoverOptions{
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
	}
}
