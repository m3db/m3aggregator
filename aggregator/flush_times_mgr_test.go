// Copyright (c) 2016 Uber Technologies, Inc.
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

package aggregator

import (
	schema "github.com/m3db/m3aggregator/generated/proto/flush"
	"github.com/m3db/m3x/watch"
)

type openShardSetIDFn func(shardSetID uint32) error
type getFlushTimesFn func() (*schema.ShardSetFlushTimes, error)
type watchFlushTimesFn func() (xwatch.Watch, error)
type storeAsyncFn func(value *schema.ShardSetFlushTimes) error

type mockFlushTimesManager struct {
	openShardSetIDFn  openShardSetIDFn
	getFlushTimesFn   getFlushTimesFn
	watchFlushTimesFn watchFlushTimesFn
	storeAsyncFn      storeAsyncFn
}

func (m *mockFlushTimesManager) Reset() error { return nil }

func (m *mockFlushTimesManager) Open(shardSetID uint32) error {
	return m.openShardSetIDFn(shardSetID)
}

func (m *mockFlushTimesManager) Get() (*schema.ShardSetFlushTimes, error) {
	return m.getFlushTimesFn()
}

func (m *mockFlushTimesManager) Watch() (xwatch.Watch, error) {
	return m.watchFlushTimesFn()
}

func (m *mockFlushTimesManager) StoreAsync(value *schema.ShardSetFlushTimes) error {
	return m.storeAsyncFn(value)
}

func (m *mockFlushTimesManager) Close() error { return nil }
