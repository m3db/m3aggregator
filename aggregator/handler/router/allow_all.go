package router

import "github.com/m3db/m3aggregator/aggregator/handler/common"

type allowAllRouter struct {
	queue common.Queue
}

// NewAllowAllRouter creates a new router that routes all data to the backend queue.
func NewAllowAllRouter(queue common.Queue) Router {
	return &allowAllRouter{queue: queue}
}

func (r *allowAllRouter) Route(shard uint32, buffer *common.RefCountedBuffer) error {
	return r.queue.Enqueue(buffer)
}

func (r *allowAllRouter) Close() { r.queue.Close() }
