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
	"github.com/m3db/m3aggregator/sharding"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const (
	testNumShards = 1024
)

func TestNewShardedHandlerEmptyShards(t *testing.T) {
	_, err := NewShardedHandler(nil, testNumShards, NewOptions())
	require.Equal(t, errEmptyBackendServerShards, err)
}

func TestShardedHandlerHandle(t *testing.T) {
	handled := make([]uint32, testNumShards)
	handlers := make([]aggregator.Handler, testNumShards)
	assignedShards := []uint32{130, 20, 240}
	for _, shard := range assignedShards {
		shard := shard
		handlers[shard] = &mockHandler{
			handleFn: func(b aggregator.ShardedBuffer) error {
				handled[shard] = b.Shard
				return nil
			},
		}
	}
	h := &shardedHandler{handlers: handlers}
	for _, shard := range assignedShards {
		h.Handle(aggregator.ShardedBuffer{Shard: shard})
	}
	expected := make([]uint32, testNumShards)
	for _, assigned := range assignedShards {
		expected[assigned] = assigned
	}
	require.Equal(t, expected, handled)
}

func TestShardedHandlerShardNotAssigned(t *testing.T) {
	h := &shardedHandler{
		handlers: make([]aggregator.Handler, testNumShards),
		metrics:  newShardedHandlerMetrics(tally.NoopScope),
	}
	buf := aggregator.ShardedBuffer{
		Shard:            64,
		RefCountedBuffer: testRefCountedBuffer(),
	}
	require.Error(t, h.Handle(buf))
	require.Panics(t, func() { buf.DecRef() })
}

func TestShardedHandlerClose(t *testing.T) {
	shards := []BackendServerShard{
		{
			Range:   testShardSet(t, "0..63"),
			Servers: []string{"localhost:0"},
		},
		{
			Range:   testShardSet(t, "64..127"),
			Servers: []string{"localhost:0"},
		},
	}
	h, err := NewShardedHandler(shards, testNumShards, NewOptions())
	require.NoError(t, err)

	// Confirm the handler can close successfully.
	h.Close()

	// Closing for a second time is a no op.
	h.Close()
}

func testShardSet(t *testing.T, str string) sharding.ShardSet {
	ss := make(sharding.ShardSet, 1024)
	require.NoError(t, ss.ParseRange(str))
	return ss
}

type handleFn func(buffer aggregator.ShardedBuffer) error

type mockHandler struct {
	handleFn handleFn
}

func (h *mockHandler) Handle(buffer aggregator.ShardedBuffer) error {
	return h.handleFn(buffer)
}

func (h *mockHandler) Close() {}
