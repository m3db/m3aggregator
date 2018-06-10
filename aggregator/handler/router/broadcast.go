package router

import (
	"github.com/m3db/m3aggregator/aggregator/handler/common"
	"github.com/m3db/m3x/errors"
)

type broadcastRouter struct {
	routers []Router
}

// NewBroadcastRouter creates a broadcast router.
func NewBroadcastRouter(routers []Router) Router {
	return &broadcastRouter{routers: routers}
}

func (r *broadcastRouter) Route(shard uint32, buffer *common.RefCountedBuffer) error {
	multiErr := errors.NewMultiError()
	for _, router := range r.routers {
		buffer.IncRef()
		if err := router.Route(shard, buffer); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	buffer.DecRef()
	return multiErr.FinalError()
}

func (r *broadcastRouter) Close() {
	for _, router := range r.routers {
		router.Close()
	}
}
