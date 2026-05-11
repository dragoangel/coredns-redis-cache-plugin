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
		Help:      "The count of errors when reading entries from Redis, bucketed by reason (timeout, connection, other).",
	}, []string{"server", "reason"})

	cacheResponseMismatches = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: plugin.Namespace,
		Subsystem: "redis_cache",
		Name:      "response_mismatches_total",
		Help:      "The count of upstream replies whose question did not match the original request and were therefore refused for caching. Non-zero suggests a misbehaving forwarder upstream or an attempted cache-poisoning probe.",
	}, []string{"server"})

	cacheSetErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: plugin.Namespace,
		Subsystem: "redis_cache",
		Name:      "set_errors_total",
		Help:      "The count of errors when adding entries to Redis, bucketed by reason (timeout, connection, other).",
	}, []string{"server", "reason"})

	cacheCollisions = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: plugin.Namespace,
		Subsystem: "redis_cache",
		Name:      "collisions_total",
		Help:      "The count of cache hits whose stored question did not match the request.",
	}, []string{"server"})

	cacheEncodeErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: plugin.Namespace,
		Subsystem: "redis_cache",
		Name:      "encode_errors_total",
		Help:      "The count of DNS messages that could not be serialized to wire format and so were not cached.",
	}, []string{"server"})
)
