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
	"fmt"
	"sync"
	"time"

	schema "github.com/m3db/m3aggregator/generated/proto/flush"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/sync"

	"github.com/uber-go/tally"
)

const (
	defaultInitialFlushTimesCapacity = 16
)

type followerFlushManagerMetrics struct {
	watchCreateErrors  tally.Counter
	unmarshalErrors    tally.Counter
	shardNotFound      tally.Counter
	resolutionNotFound tally.Counter
	kvUpdateFlush      tally.Counter
	forcedFlush        tally.Counter
}

func newFollowerFlushManagerMetrics(scope tally.Scope) followerFlushManagerMetrics {
	return followerFlushManagerMetrics{
		watchCreateErrors:  scope.Counter("watch-create-errors"),
		unmarshalErrors:    scope.Counter("unmarshal-errors"),
		shardNotFound:      scope.Counter("shard-not-found"),
		resolutionNotFound: scope.Counter("resolution-not-found"),
		kvUpdateFlush:      scope.Counter("kv-update-flush"),
		forcedFlush:        scope.Counter("forced-flush"),
	}
}

type followerFlushManager struct {
	sync.RWMutex

	nowFn                 clock.NowFn
	checkEvery            time.Duration
	workers               xsync.WorkerPool
	flushTimesKeyFmt      string
	flushTimesStore       kv.Store
	maxNoFlushDuration    time.Duration
	forcedFlushWindowSize time.Duration
	logger                xlog.Logger
	scope                 tally.Scope

	doneCh            <-chan struct{}
	proto             schema.ShardSetFlushTimes
	flushTimesKey     string
	flushTimesUpdated bool
	lastFlushed       time.Time
	flushTask         *followerFlushTask
	sleepFn           sleepFn
	metrics           followerFlushManagerMetrics
}

func newFollowerFlushManager(
	doneCh <-chan struct{},
	opts FlushManagerOptions,
) roleBasedFlushManager {
	nowFn := opts.ClockOptions().NowFn()
	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope()
	mgr := &followerFlushManager{
		nowFn:                 nowFn,
		checkEvery:            opts.CheckEvery(),
		workers:               opts.WorkerPool(),
		flushTimesKeyFmt:      opts.FlushTimesKeyFmt(),
		flushTimesStore:       opts.FlushTimesStore(),
		maxNoFlushDuration:    opts.MaxNoFlushDuration(),
		forcedFlushWindowSize: opts.ForcedFlushWindowSize(),
		logger:                instrumentOpts.Logger(),
		scope:                 scope,
		doneCh:                doneCh,
		lastFlushed:           nowFn(),
		sleepFn:               time.Sleep,
		metrics:               newFollowerFlushManagerMetrics(scope),
	}
	mgr.flushTask = &followerFlushTask{mgr: mgr}
	return mgr
}

func (mgr *followerFlushManager) Open(shardSetID string) {
	mgr.Lock()
	defer mgr.Unlock()

	mgr.flushTimesKey = fmt.Sprintf(mgr.flushTimesKeyFmt, shardSetID)
	go mgr.watchFlushTimes()
}

// NB(xichen): no actions needed for initializing the follower flush manager.
func (mgr *followerFlushManager) Init([]*flushBucket) {}

func (mgr *followerFlushManager) Prepare(buckets []*flushBucket) (flushTask, time.Duration) {
	var (
		shouldFlush        = false
		flushersByInterval []flushersGroup
	)
	// NB(xichen): a flush is triggered in the following scenarios:
	// * The flush times persisted in kv have been updated since last flush.
	// * Sufficient time (a.k.a. maxNoFlushDuration) has elapsed since last flush.
	mgr.Lock()
	if mgr.flushTimesUpdated {
		mgr.metrics.kvUpdateFlush.Inc(1)
		shouldFlush = true
		mgr.flushTimesUpdated = false
		flushersByInterval = mgr.flushersFromKVUpdateWithLock(buckets)
	} else {
		now := mgr.nowFn()
		durationSinceLastFlush := now.Sub(mgr.lastFlushed)
		if durationSinceLastFlush > mgr.maxNoFlushDuration {
			mgr.metrics.forcedFlush.Inc(1)
			shouldFlush = true
			flushBeforeNanos := now.Add(-mgr.maxNoFlushDuration).Add(mgr.forcedFlushWindowSize).UnixNano()
			flushersByInterval = mgr.flushersFromForcedFlushWithLock(buckets, flushBeforeNanos)
		}
	}
	mgr.Unlock()

	if !shouldFlush {
		return nil, mgr.checkEvery
	}
	mgr.flushTask.flushersByInterval = flushersByInterval
	return mgr.flushTask, 0
}

// NB(xichen): the follower flush manager flushes data based on the flush times
// stored in kv and does not need to take extra actions when a new bucket is added.
func (mgr *followerFlushManager) OnBucketAdded(int, *flushBucket) {}

func (mgr *followerFlushManager) flushersFromKVUpdateWithLock(buckets []*flushBucket) []flushersGroup {
	flushersByInterval := make([]flushersGroup, len(buckets))
	for i, bucket := range buckets {
		flushersByInterval[i].interval = bucket.interval
		flushersByInterval[i].duration = bucket.duration
		flushersByInterval[i].flushers = make([]flusherWithTime, 0, defaultInitialFlushTimesCapacity)
		for _, flusher := range bucket.flushers {
			shard := flusher.Shard()
			shardFlushTimes, exists := mgr.proto.ByShard[shard]
			if !exists {
				mgr.metrics.shardNotFound.Inc(1)
				mgr.logger.WithFields(
					xlog.NewLogField("shard", shard),
				).Warn("shard not found in flush times")
				continue
			}
			resolution := flusher.Resolution()
			lastFlushedAtNanos, exists := shardFlushTimes.ByResolution[int64(resolution)]
			if !exists {
				mgr.metrics.resolutionNotFound.Inc(1)
				mgr.logger.WithFields(
					xlog.NewLogField("shard", shard),
					xlog.NewLogField("resolution", resolution.String()),
				).Warn("resolution not found in flush times")
				continue
			}
			newFlushTarget := flusherWithTime{
				flusher:          flusher,
				flushBeforeNanos: lastFlushedAtNanos,
			}
			flushersByInterval[i].flushers = append(flushersByInterval[i].flushers, newFlushTarget)
		}
	}
	return flushersByInterval
}

func (mgr *followerFlushManager) flushersFromForcedFlushWithLock(
	buckets []*flushBucket,
	flushBeforeNanos int64,
) []flushersGroup {
	flushersByInterval := make([]flushersGroup, len(buckets))
	for i, bucket := range buckets {
		flushersByInterval[i].interval = bucket.interval
		flushersByInterval[i].duration = bucket.duration
		flushersByInterval[i].flushers = make([]flusherWithTime, 0, defaultInitialFlushTimesCapacity)
		for _, flusher := range bucket.flushers {
			newFlushTarget := flusherWithTime{
				flusher:          flusher,
				flushBeforeNanos: flushBeforeNanos,
			}
			flushersByInterval[i].flushers = append(flushersByInterval[i].flushers, newFlushTarget)
		}
	}
	return flushersByInterval
}

func (mgr *followerFlushManager) watchFlushTimes() {
	var (
		throttlePeriod  = time.Second
		flushTimesWatch kv.ValueWatch
		err             error
	)

	for {
		if flushTimesWatch == nil {
			flushTimesWatch, err = mgr.flushTimesStore.Watch(mgr.flushTimesKey)
			if err != nil {
				mgr.metrics.watchCreateErrors.Inc(1)
				mgr.logger.WithFields(
					xlog.NewLogField("flushTimesKey", mgr.flushTimesKey),
					xlog.NewLogErrField(err),
				).Error("error creating flush times watch")
				mgr.sleepFn(throttlePeriod)
				continue
			}
		}

		select {
		case <-flushTimesWatch.C():
		case <-mgr.doneCh:
			return
		}

		value := flushTimesWatch.Get()
		mgr.Lock()
		mgr.proto.Reset()
		if err = value.Unmarshal(&mgr.proto); err != nil {
			mgr.metrics.unmarshalErrors.Inc(1)
			mgr.logger.WithFields(
				xlog.NewLogField("flushTimesKey", mgr.flushTimesKey),
				xlog.NewLogErrField(err),
			).Error("flush times unmarshal error")
			mgr.Unlock()
			continue
		}
		mgr.flushTimesUpdated = true
		mgr.Unlock()
	}
}

type followerFlushTask struct {
	mgr                *followerFlushManager
	flushersByInterval []flushersGroup
}

func (t *followerFlushTask) Run() {
	var (
		mgr       = t.mgr
		wgWorkers sync.WaitGroup
	)
	for _, group := range t.flushersByInterval {
		start := mgr.nowFn()
		for _, flusherWithTime := range group.flushers {
			flusherWithTime := flusherWithTime
			wgWorkers.Add(1)
			mgr.workers.Go(func() {
				flusherWithTime.flusher.FlushBefore(flusherWithTime.flushBeforeNanos)
				wgWorkers.Done()
			})
		}
		wgWorkers.Wait()
		group.duration.Record(mgr.nowFn().Sub(start))
	}
}

type flusherWithTime struct {
	flusher          PeriodicFlusher
	flushBeforeNanos int64
}

type flushersGroup struct {
	interval time.Duration
	duration tally.Timer
	flushers []flusherWithTime
}
