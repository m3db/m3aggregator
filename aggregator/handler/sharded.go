// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package handler

import (
	"errors"
	"fmt"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3aggregator/sharding"

	"github.com/uber-go/tally"
)

var (
	errEmptyBackendServerShards = errors.New("empty backend server shards")
)

// BackendServerShard contains a backend server shard.
type BackendServerShard struct {
	// Name of this shard for logging and metrics.
	Name string `yaml:"name"`

	// Shard range.
	Range sharding.ShardSet `yaml:"range" validate:"nonzero"`

	// Servers owning the shard.
	Servers []string `yaml:"servers" validate:"nonzero"`
}

type shardedHandlerMetrics struct {
	shardNotAssigned tally.Counter
}

func newShardedHandlerMetrics(scope tally.Scope) shardedHandlerMetrics {
	return shardedHandlerMetrics{
		shardNotAssigned: scope.Counter("shard-not-assigned"),
	}
}

type shardedHandler struct {
	handlers []aggregator.Handler
	metrics  shardedHandlerMetrics
}

// NewShardedHandler creates a new sharded handler.
func NewShardedHandler(
	shards []BackendServerShard,
	totalShards int,
	opts Options,
) (aggregator.Handler, error) {
	if len(shards) == 0 {
		return nil, errEmptyBackendServerShards
	}
	var (
		// Divide overall queue size among all shards.
		shardQueueSize = opts.QueueSize() / len(shards)
		handlers       = make([]aggregator.Handler, totalShards)
		instrumentOpts = opts.InstrumentOptions()
		scope          = instrumentOpts.MetricsScope()
	)
	for _, shard := range shards {
		handlerScope := scope.Tagged(map[string]string{
			"shard-name": shard.Name,
		})
		iOpts := instrumentOpts.SetMetricsScope(handlerScope)
		handlerOpts := opts.SetInstrumentOptions(iOpts).SetQueueSize(shardQueueSize)
		h, err := NewForwardHandler(shard.Servers, handlerOpts)
		if err != nil {
			return nil, err
		}
		for s := range shard.Range {
			handlers[s] = h
		}
	}
	return &shardedHandler{
		handlers: handlers,
		metrics:  newShardedHandlerMetrics(scope),
	}, nil
}

func (h *shardedHandler) Handle(buffer aggregator.ShardedBuffer) error {
	if shard := int(buffer.Shard); shard < len(h.handlers) && h.handlers[shard] != nil {
		return h.handlers[shard].Handle(buffer)
	}
	buffer.DecRef()
	h.metrics.shardNotAssigned.Inc(1)
	return fmt.Errorf("shard %d is not assigned to any of the backend servers", buffer.Shard)
}

func (h *shardedHandler) Close() {
	for _, handler := range h.handlers {
		if handler == nil {
			continue
		}
		handler.Close()
	}
}
