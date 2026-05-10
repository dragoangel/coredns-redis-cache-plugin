package redis_cache

import (
	"github.com/coredns/coredns/plugin"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	cacheHits = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: plugin.Namespace,
		Subsystem: "redis_cache",
		Name:      "hits_total",
		Help:      "The count of cache hits from Redis.",
	}, []string{"server"})

	cacheMisses = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: plugin.Namespace,
		Subsystem: "redis_cache",
		Name:      "misses_total",
		Help:      "The count of cache misses from Redis.",
	}, []string{"server"})

	cacheReadErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: plugin.Namespace,
		Subsystem: "redis_cache",
		Name:      "get_errors_total",
		Help:      "The count of errors when reading entries from Redis.",
	}, []string{"server"})

	cacheDrops = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: plugin.Namespace,
		Subsystem: "redis_cache",
		Name:      "drops_total",
		Help:      "The count of responses not cached because the reply's question doesn't match the request.",
	}, []string{"server"})

	redisErr = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: plugin.Namespace,
		Subsystem: "redis_cache",
		Name:      "set_errors_total",
		Help:      "The count of errors when adding entries to Redis.",
	}, []string{"server"})
)
