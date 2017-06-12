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
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/require"
)

func TestBroadcastHandler(t *testing.T) {
	pool := msgpack.NewBufferedEncoderPool(pool.NewObjectPoolOptions().SetSize(1))
	pool.Init(func() msgpack.BufferedEncoder {
		return msgpack.NewPooledBufferedEncoder(pool)
	})

	be1 := pool.Get()

	bh := NewBroadcastHandler([]aggregator.Handler{NewBlackholeHandler()})

	require.NoError(t, bh.Handle(be1))

	be2 := pool.Get()
	require.Equal(t, be1, be2)
}

func TestBroadcastHandlerWithHandlerError(t *testing.T) {
	pool := msgpack.NewBufferedEncoderPool(pool.NewObjectPoolOptions().SetSize(1))
	pool.Init(func() msgpack.BufferedEncoder {
		return msgpack.NewPooledBufferedEncoder(pool)
	})

	be1 := pool.Get()
	be1.EncodeBytes([]byte{'a', 'b', 'c', 'd'})

	bh := NewBroadcastHandler([]aggregator.Handler{NewLoggingHandler(instrument.NewOptions()), NewBlackholeHandler()})

	require.Error(t, bh.Handle(be1))

	be2 := pool.Get()
	require.Equal(t, be1, be2)
}

func TestBroadcastHandlerWithNoHandlers(t *testing.T) {
	pool := msgpack.NewBufferedEncoderPool(pool.NewObjectPoolOptions().SetSize(1))
	pool.Init(func() msgpack.BufferedEncoder {
		return msgpack.NewPooledBufferedEncoder(pool)
	})

	be1 := pool.Get()

	bh := NewBroadcastHandler(nil)

	require.NoError(t, bh.Handle(be1))

	be2 := pool.Get()
	require.Equal(t, be1, be2)
}
