package redis_cache

import (
	"context"
	"fmt"
	"math/rand/v2"

	"github.com/redis/go-redis/v9"
)

// readReplicaPool holds N>=2 read-only Redis clients (one per
// explicit-replica address) and picks one at random per call. Random over
// a sufficiently busy fleet converges to ~1/N load per replica; using
// random instead of strict round-robin avoids cross-instance synchronization
// patterns where multiple CoreDNS pods step in lockstep onto the same
// replica.
type readReplicaPool struct {
	clients []*redis.Client
}

// pick returns a client at random.
func (p *readReplicaPool) pick() *redis.Client {
	return p.clients[rand.IntN(len(p.clients))]
}

// pipeline starts a pipeline on a randomly-picked replica.
func (p *readReplicaPool) pipeline() redis.Pipeliner {
	return p.pick().Pipeline()
}

// ping verifies each replica is reachable. Errors are logged at warning
// level and not propagated, matching the existing per-client behaviour:
// a temporarily unreachable replica should not block plugin start, and
// the random pick will route around it on subsequent reads.
func (p *readReplicaPool) ping(ctx context.Context) {
	for _, c := range p.clients {
		if err := c.Ping(ctx).Err(); err != nil {
			log.Warningf("Read replica %s ping failed (will retry on demand): %s", c.Options().Addr, err)
		}
	}
}

// close shuts down every replica client and aggregates errors.
func (p *readReplicaPool) close() error {
	var errs []error
	for _, c := range p.clients {
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}
	return nil
}
