package router

import "github.com/m3db/m3aggregator/aggregator/handler/common"

// Router routes data to the corresponding backends.
type Router interface {
	// Route routes a buffer for a given shard. It should decrement
	// the reference count of the buffer is done being used.
	Route(shard uint32, buffer *common.RefCountedBuffer) error

	// Close closes the router.
	Close()
}
