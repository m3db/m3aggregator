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
	"sync/atomic"
	"testing"
	"time"

	schema "github.com/m3db/m3aggregator/generated/proto/flush"
	"github.com/m3db/m3cluster/kv/mem"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

var (
	testFlushBuckets = []*flushBucket{
		&flushBucket{
			interval: time.Second,
			flushers: []PeriodicFlusher{
				&mockFlusher{
					shard:            0,
					resolution:       time.Second,
					flushInterval:    time.Second,
					lastFlushedNanos: 3663000000000,
				},
				&mockFlusher{
					shard:            1,
					resolution:       time.Second,
					flushInterval:    time.Second,
					lastFlushedNanos: 3668000000000,
				},
			},
		},
		&flushBucket{
			interval: time.Minute,
			flushers: []PeriodicFlusher{
				&mockFlusher{
					shard:            0,
					resolution:       time.Minute,
					flushInterval:    time.Minute,
					lastFlushedNanos: 3660000000000,
				},
			},
		},
		&flushBucket{
			interval: time.Hour,
			flushers: []PeriodicFlusher{
				&mockFlusher{
					shard:            2,
					resolution:       time.Hour,
					flushInterval:    time.Hour,
					lastFlushedNanos: 3600000000000,
				},
			},
		},
	}

	testFlushTimes = &schema.ShardSetFlushTimes{
		ByShard: map[uint32]*schema.ShardFlushTimes{
			0: &schema.ShardFlushTimes{
				ByResolution: map[int64]int64{
					1000000000:  3663000000000,
					60000000000: 3660000000000,
				},
			},
			1: &schema.ShardFlushTimes{
				ByResolution: map[int64]int64{
					1000000000: 3668000000000,
				},
			},
			2: &schema.ShardFlushTimes{
				ByResolution: map[int64]int64{
					3600000000000: 3600000000000,
				},
			},
		},
	}
)

func TestLeaderFlushManagerOpen(t *testing.T) {
	flushTimesKeyFmt := "/shardset/%s/flush"
	opts := NewFlushManagerOptions().SetFlushTimesKeyFmt(flushTimesKeyFmt)
	mgr := newLeaderFlushManager(opts).(*leaderFlushManager)
	mgr.Open(testShardSetID)
	require.Equal(t, "/shardset/testShardSet/flush", mgr.flushTimesKey)
}

func TestLeaderFlushManagerInit(t *testing.T) {
	now := time.Unix(1234, 0)
	nowFn := func() time.Time { return now }
	opts := NewFlushManagerOptions().SetJitterEnabled(false)
	mgr := newLeaderFlushManager(opts).(*leaderFlushManager)
	mgr.nowFn = nowFn

	mgr.Init(testFlushBuckets)
	expectedFlushTimes := []flushMetadata{
		{timeNanos: 1235000000000, bucketIdx: 0},
		{timeNanos: 1294000000000, bucketIdx: 1},
		{timeNanos: 4834000000000, bucketIdx: 2},
	}
	require.Equal(t, flushMetadataHeap(expectedFlushTimes), mgr.flushTimes)
}

func TestLeaderFlushManagerPrepareNoFlushNoPersist(t *testing.T) {
	now := time.Unix(1234, 0)
	nowFn := func() time.Time { return now }
	opts := NewFlushManagerOptions().SetJitterEnabled(false)
	mgr := newLeaderFlushManager(opts).(*leaderFlushManager)
	mgr.nowFn = nowFn
	mgr.lastPersistAt = now

	mgr.Init(testFlushBuckets)
	now = now.Add(100 * time.Millisecond)
	flushTask, dur := mgr.Prepare(testFlushBuckets)
	require.Nil(t, flushTask)
	require.Equal(t, 900*time.Millisecond, dur)
}

func TestLeaderFlushManagerPrepareNoFlushWithPersist(t *testing.T) {
	now := time.Unix(1234, 0)
	nowFn := func() time.Time { return now }
	opts := NewFlushManagerOptions().
		SetJitterEnabled(false).
		SetFlushTimesPersistEvery(time.Second)
	mgr := newLeaderFlushManager(opts).(*leaderFlushManager)
	mgr.nowFn = nowFn
	mgr.lastPersistAt = now.Add(-2 * time.Second)
	mgr.flushedSincePersist = true

	mgr.Init(testFlushBuckets)
	flushTask, dur := mgr.Prepare(testFlushBuckets)
	require.NotNil(t, flushTask)
	require.Equal(t, time.Duration(0), dur)
	task := flushTask.(*leaderFlushTask)
	require.False(t, task.shouldFlush)
	require.Nil(t, task.flushers)
	require.True(t, task.shouldPersist)
	require.Equal(t, testFlushTimes, task.flushTimes)
	require.Equal(t, now, mgr.lastPersistAt)
	require.False(t, mgr.flushedSincePersist)
}

func TestLeaderFlushManagerPrepareWithFlushAndPersist(t *testing.T) {
	now := time.Unix(1234, 0)
	nowFn := func() time.Time { return now }
	opts := NewFlushManagerOptions().
		SetJitterEnabled(false).
		SetFlushTimesPersistEvery(time.Second)
	mgr := newLeaderFlushManager(opts).(*leaderFlushManager)
	mgr.nowFn = nowFn
	mgr.lastPersistAt = now
	mgr.flushedSincePersist = true
	mgr.Init(testFlushBuckets)

	now = now.Add(2 * time.Second)
	flushTask, dur := mgr.Prepare(testFlushBuckets)

	expectedFlushTimes := []flushMetadata{
		{timeNanos: 1236000000000, bucketIdx: 0},
		{timeNanos: 4834000000000, bucketIdx: 2},
		{timeNanos: 1294000000000, bucketIdx: 1},
	}
	require.NotNil(t, flushTask)
	require.Equal(t, time.Duration(0), dur)
	task := flushTask.(*leaderFlushTask)
	require.True(t, task.shouldFlush)
	require.Equal(t, testFlushBuckets[0].flushers, task.flushers)
	require.True(t, task.shouldPersist)
	require.Equal(t, testFlushTimes, task.flushTimes)
	require.Equal(t, flushMetadataHeap(expectedFlushTimes), mgr.flushTimes)
}

func TestLeaderFlushManagerOnBucketAdded(t *testing.T) {
	now := time.Unix(1234, 0)
	nowFn := func() time.Time { return now }
	opts := NewFlushManagerOptions().SetJitterEnabled(false)
	mgr := newLeaderFlushManager(opts).(*leaderFlushManager)
	mgr.nowFn = nowFn

	mgr.OnBucketAdded(0, testFlushBuckets[0])
	expectedFlushTimes := []flushMetadata{
		{timeNanos: 1235000000000, bucketIdx: 0},
	}
	require.Equal(t, flushMetadataHeap(expectedFlushTimes), mgr.flushTimes)
}

func TestLeaderFlushTaskRun(t *testing.T) {
	var flushed int32
	flushers := []PeriodicFlusher{
		&mockFlusher{
			flushFn: func() { atomic.AddInt32(&flushed, 1) },
		},
		&mockFlusher{
			flushFn: func() { atomic.AddInt32(&flushed, 1) },
		},
	}
	flushTimes := &schema.ShardSetFlushTimes{
		ByShard: map[uint32]*schema.ShardFlushTimes{
			0: &schema.ShardFlushTimes{
				ByResolution: map[int64]int64{
					1000000000:  1000,
					60000000000: 1200,
				},
			},
		},
	}
	store := mem.NewStore()
	opts := NewFlushManagerOptions().
		SetJitterEnabled(false).
		SetFlushTimesStore(store).
		SetFlushTimesPersistEvery(time.Second)
	mgr := newLeaderFlushManager(opts).(*leaderFlushManager)
	mgr.flushTimesKey = "flushTimes"

	flushTask := &leaderFlushTask{
		mgr:           mgr,
		shouldFlush:   true,
		duration:      tally.NoopScope.Timer("foo"),
		flushers:      flushers,
		shouldPersist: true,
		flushTimes:    flushTimes,
	}
	flushTask.Run()
	require.Equal(t, int32(2), flushed)
	for {
		v, err := store.Get(mgr.flushTimesKey)
		if err == nil {
			var actual schema.ShardSetFlushTimes
			require.NoError(t, v.Unmarshal(&actual))
			require.Equal(t, *flushTimes, actual)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}
