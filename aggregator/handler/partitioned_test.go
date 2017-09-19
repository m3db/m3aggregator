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
	"testing"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3aggregator/partitioning"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestNewPartitionedHandlerEmptyPartitions(t *testing.T) {
	_, err := NewPartitionedHandler(nil, NewOptions())
	require.Equal(t, errEmptyPartitions, err)
}

func TestNewPartitionedHandlerContains(t *testing.T) {
	numPartitions := 3
	handled := make([]uint32, numPartitions)
	handlers := []handlerWithPartition{
		{
			Partitions: testPartitionSet(t, "0..63"),
			Handler: &mockHandler{
				handleFn: func(b aggregator.PartitionedBuffer) error {
					handled[0] = b.Partition
					return nil
				},
			},
		},
		{
			Partitions: testPartitionSet(t, "127..155"),
			Handler: &mockHandler{
				handleFn: func(b aggregator.PartitionedBuffer) error {
					handled[1] = b.Partition
					return nil
				},
			},
		},
		{
			Partitions: testPartitionSet(t, "200..300"),
			Handler: &mockHandler{
				handleFn: func(b aggregator.PartitionedBuffer) error {
					handled[2] = b.Partition
					return nil
				},
			},
		},
	}
	h := &partitionedHandler{handlers: handlers}
	for _, partition := range []uint32{130, 20, 240} {
		h.Handle(aggregator.PartitionedBuffer{Partition: partition})
	}
	expected := []uint32{20, 130, 240}
	require.Equal(t, expected, handled)
}

func TestNewPartitionedHandlerPartitionNotOwned(t *testing.T) {
	handlers := []handlerWithPartition{
		{Partitions: testPartitionSet(t, "0..63")},
	}
	h := &partitionedHandler{
		handlers: handlers,
		metrics:  newPartitionedHandlerMetrics(tally.NoopScope),
	}
	buf := aggregator.PartitionedBuffer{
		Partition:        64,
		RefCountedBuffer: testRefCountedBuffer(),
	}
	require.Error(t, h.Handle(buf))
	require.Panics(t, func() { buf.DecRef() })
}

func TestNewPartitionedHandlerClose(t *testing.T) {
	partitions := []BackendServerPartition{
		{
			Range:   testPartitionSet(t, "0..63"),
			Servers: []string{"localhost:0"},
		},
		{
			Range:   testPartitionSet(t, "64..127"),
			Servers: []string{"localhost:0"},
		},
	}
	h, err := NewPartitionedHandler(partitions, NewOptions())
	require.NoError(t, err)

	// Confirm the handler can close successfully.
	h.Close()

	// Closing for a second time is a no op.
	h.Close()
}

func testPartitionSet(t *testing.T, str string) partitioning.PartitionSet {
	ps := make(partitioning.PartitionSet, 1024)
	require.NoError(t, ps.ParseRange(str))
	return ps
}

type handleFn func(buffer aggregator.PartitionedBuffer) error

type mockHandler struct {
	handleFn handleFn
}

func (h *mockHandler) Handle(buffer aggregator.PartitionedBuffer) error {
	return h.handleFn(buffer)
}

func (h *mockHandler) Close() {}
