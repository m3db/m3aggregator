package router

import (
	"fmt"

	"github.com/m3db/m3aggregator/aggregator/handler/common"
	"github.com/m3db/m3aggregator/sharding"

	"github.com/uber-go/tally"
)

type shardedRouterMetrics struct {
	routeNotFound tally.Counter
}

func newShardedRouterMetrics(scope tally.Scope) shardedRouterMetrics {
	return shardedRouterMetrics{
		routeNotFound: scope.Counter("route-not-found"),
	}
}

type shardedRouter struct {
	queues  []common.Queue
	metrics shardedRouterMetrics
}

// ShardedQueue is a backend queue responsible for a set of shards.
type ShardedQueue struct {
	sharding.ShardSet
	common.Queue
}

// NewShardedRouter creates a sharded router.
func NewShardedRouter(
	shardedQueues []ShardedQueue,
	totalShards int,
	scope tally.Scope,
) Router {
	queues := make([]common.Queue, totalShards)
	for _, q := range shardedQueues {
		for shard := range q.ShardSet {
			queues[shard] = q.Queue
		}
	}
	return &shardedRouter{
		queues:  queues,
		metrics: newShardedRouterMetrics(scope),
	}
}

func (r *shardedRouter) Route(shard uint32, buffer *common.RefCountedBuffer) error {
	if int(shard) < len(r.queues) && r.queues[shard] != nil {
		return r.queues[shard].Enqueue(buffer)
	}
	buffer.DecRef()
	r.metrics.routeNotFound.Inc(1)
	return fmt.Errorf("shard %d is not assigned to any of the backend queues", shard)
}

func (r *shardedRouter) Close() {
	for _, queue := range r.queues {
		// If the backend is associated with a subset of shards instead of the
		// full shardset, the unused shards will not have an associated queue.
		if queue != nil {
			queue.Close()
		}
	}
}
