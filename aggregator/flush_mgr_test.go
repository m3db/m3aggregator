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

package aggregator

import (
	"testing"
	"time"

	"github.com/m3db/m3x/clock"

	"github.com/stretchr/testify/require"
)

var (
	testShardSetID = "testShardSet"
)

func TestFlushManagerRegisterClosed(t *testing.T) {
	mgr, _ := testFlushManager()
	mgr.status = flushManagerClosed
	require.Equal(t, errFlushManagerNotOpenOrClosed, mgr.Register(nil))
}

func TestFlushManagerRegisterSuccess(t *testing.T) {
	mgr, now := testFlushManager()
	*now = time.Unix(1234, 0)

	var (
		bucketIndices []int
		buckets       []*flushBucket
	)
	mgr.leaderMgr = &mockRoleBasedFlushManager{
		openFn: func(string) {},
	}
	mgr.followerMgr = &mockRoleBasedFlushManager{
		openFn: func(string) {},
		onBucketAddedFn: func(bucketIdx int, bucket *flushBucket) {
			bucketIndices = append(bucketIndices, bucketIdx)
			buckets = append(buckets, bucket)
		},
	}
	flushers := []PeriodicFlusher{
		&mockFlusher{flushInterval: time.Second},
		&mockFlusher{flushInterval: time.Minute},
		&mockFlusher{flushInterval: time.Second},
		&mockFlusher{flushInterval: time.Hour},
	}

	require.NoError(t, mgr.Open(testShardSetID))
	for _, flusher := range flushers {
		require.NoError(t, mgr.Register(flusher))
	}

	expectedBuckets := []*flushBucket{
		&flushBucket{
			interval: time.Second,
			flushers: []PeriodicFlusher{flushers[0], flushers[2]},
		},
		&flushBucket{
			interval: time.Minute,
			flushers: []PeriodicFlusher{flushers[1]},
		},
		&flushBucket{
			interval: time.Hour,
			flushers: []PeriodicFlusher{flushers[3]},
		},
	}
	require.Equal(t, len(expectedBuckets), len(mgr.buckets))
	for i := 0; i < len(expectedBuckets); i++ {
		require.Equal(t, expectedBuckets[i].interval, mgr.buckets[i].interval)
		require.Equal(t, expectedBuckets[i].flushers, mgr.buckets[i].flushers)
	}
	for i := 0; i < len(expectedBuckets); i++ {
		require.Equal(t, i, bucketIndices[i])
		require.Equal(t, expectedBuckets[i].interval, buckets[i].interval)
		require.Equal(t, expectedBuckets[i].flushers, buckets[i].flushers)
	}
}

func TestFlushManagerCloseSuccess(t *testing.T) {
	opts, _ := testFlushManagerOptions()
	opts = opts.SetCheckEvery(time.Second)
	mgr := NewFlushManager(opts).(*flushManager)
	mgr.status = flushManagerOpen

	// Wait a little for the flush goroutine to start.
	time.Sleep(100 * time.Millisecond)

	mgr.Close()
	require.Equal(t, flushManagerClosed, mgr.status)
	require.Panics(t, func() { mgr.wgFlush.Done() })
}

func TestFlushManagerFlush(t *testing.T) {
	opts := NewFlushManagerOptions().
		SetCheckEvery(100 * time.Millisecond).
		SetJitterEnabled(false)
	mgr := NewFlushManager(opts).(*flushManager)

	var captured []*flushBucket
	mgr.electionMgr = &mockElectionManager{
		electionStatus: FollowerStatus,
	}
	mgr.leaderMgr = &mockRoleBasedFlushManager{
		openFn: func(string) {},
	}
	mgr.followerMgr = &mockRoleBasedFlushManager{
		openFn: func(string) {},
		prepareFn: func(buckets []*flushBucket) (flushTask, time.Duration) {
			captured = buckets
			return nil, 0
		},
		onBucketAddedFn: func(bucketIdx int, bucket *flushBucket) {},
	}

	require.NoError(t, mgr.Open(testShardSetID))
	flushers := []PeriodicFlusher{
		&mockFlusher{flushInterval: 100 * time.Millisecond},
		&mockFlusher{flushInterval: 200 * time.Millisecond},
		&mockFlusher{flushInterval: 100 * time.Millisecond},
		&mockFlusher{flushInterval: 500 * time.Millisecond},
	}
	for _, flusher := range flushers {
		require.NoError(t, mgr.Register(flusher))
	}
	time.Sleep(1200 * time.Millisecond)
	mgr.Close()

	expectedBuckets := []*flushBucket{
		&flushBucket{
			interval: 100 * time.Millisecond,
			flushers: []PeriodicFlusher{flushers[0], flushers[2]},
		},
		&flushBucket{
			interval: 200 * time.Millisecond,
			flushers: []PeriodicFlusher{flushers[1]},
		},
		&flushBucket{
			interval: 500 * time.Millisecond,
			flushers: []PeriodicFlusher{flushers[3]},
		},
	}
	require.Equal(t, len(expectedBuckets), len(captured))
	for i := range expectedBuckets {
		require.Equal(t, expectedBuckets[i].interval, captured[i].interval)
		require.Equal(t, expectedBuckets[i].flushers, captured[i].flushers)
	}
}

func testFlushManager() (*flushManager, *time.Time) {
	opts, now := testFlushManagerOptions()
	return NewFlushManager(opts).(*flushManager), now
}

func testFlushManagerOptions() (FlushManagerOptions, *time.Time) {
	var now time.Time
	nowFn := func() time.Time { return now }
	clockOpts := clock.NewOptions().SetNowFn(nowFn)
	return NewFlushManagerOptions().
		SetClockOptions(clockOpts).
		SetCheckEvery(0).
		SetJitterEnabled(false), &now
}

type flushFn func()
type flushBeforeFn func(beforeNanos int64)

type mockFlusher struct {
	shard            uint32
	resolution       time.Duration
	flushInterval    time.Duration
	lastFlushedNanos int64
	flushFn          flushFn
	flushBeforeFn    flushBeforeFn
}

func (f *mockFlusher) Shard() uint32                 { return f.shard }
func (f *mockFlusher) Resolution() time.Duration     { return f.resolution }
func (f *mockFlusher) FlushInterval() time.Duration  { return f.flushInterval }
func (f *mockFlusher) LastFlushedNanos() int64       { return f.lastFlushedNanos }
func (f *mockFlusher) Flush()                        { f.flushFn() }
func (f *mockFlusher) FlushBefore(beforeNanos int64) { f.flushBeforeFn(beforeNanos) }

type flushOpenFn func(shardSetID string)
type bucketInitFn func(buckets []*flushBucket)
type bucketPrepareFn func(buckets []*flushBucket) (flushTask, time.Duration)
type onBucketAddedFn func(bucketIdx int, bucket *flushBucket)

type mockRoleBasedFlushManager struct {
	openFn          flushOpenFn
	initFn          bucketInitFn
	prepareFn       bucketPrepareFn
	onBucketAddedFn onBucketAddedFn
}

func (m *mockRoleBasedFlushManager) Open(shardSetID string) { m.openFn(shardSetID) }

func (m *mockRoleBasedFlushManager) Init(buckets []*flushBucket) {
	m.initFn(buckets)
}

func (m *mockRoleBasedFlushManager) Prepare(buckets []*flushBucket) (flushTask, time.Duration) {
	return m.prepareFn(buckets)
}

func (m *mockRoleBasedFlushManager) OnBucketAdded(bucketIdx int, bucket *flushBucket) {
	m.onBucketAddedFn(bucketIdx, bucket)
}
