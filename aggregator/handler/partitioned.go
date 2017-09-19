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
	"strconv"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3aggregator/partitioning"

	"github.com/uber-go/tally"
)

var (
	errEmptyPartitions = errors.New("empty partitions")
)

// BackendServerPartition contains a backend server partition.
type BackendServerPartition struct {
	// Partition range.
	Range partitioning.PartitionSet `validate:"nonzero"`

	// Servers owning the partition.
	Servers []string `validate:"nonzero"`
}

type partitionedHandlerMetrics struct {
	partitionNotOwned tally.Counter
}

func newPartitionedHandlerMetrics(scope tally.Scope) partitionedHandlerMetrics {
	return partitionedHandlerMetrics{
		partitionNotOwned: scope.Counter("partition-not-owned"),
	}
}

type handlerWithPartition struct {
	Partitions partitioning.PartitionSet
	Handler    aggregator.Handler
}

type partitionedHandler struct {
	handlers []handlerWithPartition
	metrics  partitionedHandlerMetrics
}

// NewPartitionedHandler creates a new partitioned handler.
func NewPartitionedHandler(
	partitions []BackendServerPartition,
	opts Options,
) (aggregator.Handler, error) {
	numPartitions := len(partitions)
	if numPartitions == 0 {
		return nil, errEmptyPartitions
	}
	var (
		// Divide overall queue size among all partitions.
		partitionQueueSize = opts.QueueSize() / numPartitions
		handlers           = make([]handlerWithPartition, 0, numPartitions)
		instrumentOpts     = opts.InstrumentOptions()
		scope              = instrumentOpts.MetricsScope()
	)
	for _, partition := range partitions {
		handlerScope := scope.Tagged(map[string]string{
			"min-partition": strconv.Itoa(partition.Range.Min()),
			"max-partition": strconv.Itoa(partition.Range.Max()),
		})
		iOpts := instrumentOpts.SetMetricsScope(handlerScope)
		handlerOpts := opts.SetInstrumentOptions(iOpts).SetQueueSize(partitionQueueSize)
		h, err := NewForwardHandler(partition.Servers, handlerOpts)
		if err != nil {
			return nil, err
		}
		handlers = append(handlers, handlerWithPartition{
			Partitions: partition.Range,
			Handler:    h,
		})
	}
	return &partitionedHandler{
		handlers: handlers,
		metrics:  newPartitionedHandlerMetrics(scope),
	}, nil
}

func (h *partitionedHandler) Handle(buffer aggregator.PartitionedBuffer) error {
	for _, handler := range h.handlers {
		if handler.Partitions.Contains(buffer.Partition) {
			return handler.Handler.Handle(buffer)
		}
	}
	buffer.DecRef()
	h.metrics.partitionNotOwned.Inc(1)
	return fmt.Errorf("partition %d is not owned by any of the backend servers", buffer.Partition)
}

func (h *partitionedHandler) Close() {
	for _, handler := range h.handlers {
		handler.Handler.Close()
	}
}
