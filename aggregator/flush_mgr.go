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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/uber-go/tally"
)

var (
	errFlushManagerClosed = errors.New("flush manager is closed")
)

// PeriodicFlusher flushes metrics periodically.
type PeriodicFlusher interface {
	// Shard returns the shard associated with the flusher.
	Shard() uint32

	// Resolution returns the resolution of metrics associated with the flusher.
	Resolution() time.Duration

	// FlushInterval returns the periodic flush interval.
	FlushInterval() time.Duration

	// LastFlushedNanos returns the last flushed timestamp.
	LastFlushedNanos() int64

	// Flush performs a flush.
	Flush()

	// FlushBefore flushes all metrics before a given timestamp.
	FlushBefore(beforeNanos int64)
}

// FlushManager manages and coordinates flushing activities across many
// periodic flushers with different flush intervals with controlled concurrency
// for flushes to minimize spikes in CPU load and reduce p99 flush latencies.
type FlushManager interface {
	// Open opens the flush manager for a given shard set.
	Open(shardSetID string) error

	// Register registers a metric list with the flush manager.
	Register(flusher PeriodicFlusher) error

	// Close closes the flush manager.
	Close() error
}

// roleBasedFlushManager manages flushing data based on their elected roles.
type roleBasedFlushManager interface {
	Open(shardSetID string)

	Init(bucketLock *sync.RWMutex, buckets []*flushBucket)

	Flush(bucketLock *sync.RWMutex, buckets []*flushBucket) (bool, time.Duration)

	OnBucketAdded(bucketIdx int, bucket *flushBucket)
}

var (
	errFlushManagerAlreadyOpenOrClosed = errors.New("flush manager is already open or closed")
	errFlushManagerNotOpenOrClosed     = errors.New("flush manager is not open or closed")
)

type flushManagerStatus int

const (
	flushManagerNotOpen flushManagerStatus = iota
	flushManagerOpen
	flushManagerClosed
)

type flushManager struct {
	sync.RWMutex

	scope      tally.Scope
	checkEvery time.Duration

	status         flushManagerStatus
	doneCh         chan struct{}
	buckets        []*flushBucket
	electionStatus electionStatus
	electionMgr    ElectionManager
	leaderMgr      roleBasedFlushManager
	followerMgr    roleBasedFlushManager
	sleepFn        sleepFn
	wgFlush        sync.WaitGroup
}

// NewFlushManager creates a new flush manager.
func NewFlushManager(opts FlushManagerOptions) FlushManager {
	if opts == nil {
		opts = NewFlushManagerOptions()
	}
	doneCh := make(chan struct{})
	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope()

	leaderMgrScope := scope.SubScope("leader")
	leaderMgrInstrumentOpts := instrumentOpts.SetMetricsScope(leaderMgrScope)
	leaderMgr := newLeaderFlushManager(opts.SetInstrumentOptions(leaderMgrInstrumentOpts))

	followerMgrScope := scope.SubScope("follower")
	followerMgrInstrumentOpts := instrumentOpts.SetMetricsScope(followerMgrScope)
	followerMgr := newFollowerFlushManager(doneCh, opts.SetInstrumentOptions(followerMgrInstrumentOpts))

	return &flushManager{
		scope:          instrumentOpts.MetricsScope(),
		checkEvery:     opts.CheckEvery(),
		doneCh:         doneCh,
		electionStatus: followerStatus,
		electionMgr:    opts.ElectionManager(),
		leaderMgr:      leaderMgr,
		followerMgr:    followerMgr,
		sleepFn:        time.Sleep,
	}
}

func (mgr *flushManager) Open(shardSetID string) error {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.status != flushManagerNotOpen {
		return errFlushManagerAlreadyOpenOrClosed
	}
	mgr.status = flushManagerOpen
	mgr.leaderMgr.Open(shardSetID)
	mgr.followerMgr.Open(shardSetID)

	if mgr.checkEvery > 0 {
		mgr.wgFlush.Add(1)
		go mgr.flush()
	}
	return nil
}

func (mgr *flushManager) Register(flusher PeriodicFlusher) error {
	mgr.Lock()
	bucket, err := mgr.bucketForWithLock(flusher)
	if err == nil {
		bucket.Add(flusher)
		mgr.Unlock()
		return nil
	}
	mgr.Unlock()
	return err
}

func (mgr *flushManager) Close() error {
	mgr.Lock()
	if mgr.status != flushManagerOpen {
		mgr.Unlock()
		return errFlushManagerNotOpenOrClosed
	}
	mgr.status = flushManagerClosed
	close(mgr.doneCh)
	mgr.Unlock()

	mgr.wgFlush.Wait()
	return nil
}

func (mgr *flushManager) bucketForWithLock(l PeriodicFlusher) (*flushBucket, error) {
	if mgr.status != flushManagerOpen {
		return nil, errFlushManagerNotOpenOrClosed
	}
	flushInterval := l.FlushInterval()
	for _, bucket := range mgr.buckets {
		if bucket.interval == flushInterval {
			return bucket, nil
		}
	}
	bucketScope := mgr.scope.SubScope("bucket").Tagged(map[string]string{
		"interval": flushInterval.String(),
	})
	bucket := newBucket(flushInterval, bucketScope)
	mgr.buckets = append(mgr.buckets, bucket)
	mgr.flushManagerWithLock().OnBucketAdded(len(mgr.buckets)-1, bucket)
	return bucket, nil
}

// NB(xichen): apparently timer.Reset() is more difficult to use than I originally
// anticipated. For now I'm simply waking up every second to check for updates. Maybe
// when I have more time I'll spend a few hours to get timer.Reset() right and switch
// to a timer.Start + timer.Stop + timer.Reset + timer.After based approach.
func (mgr *flushManager) flush() {
	defer mgr.wgFlush.Done()

	for {
		mgr.RLock()
		status := mgr.status
		electionStatus := mgr.electionStatus
		flushManager := mgr.flushManagerWithLock()
		mgr.RUnlock()
		if status == flushManagerClosed {
			return
		}

		// If the election status has changed, we need to switch the flush manager.
		newElectionStatus := mgr.electionMgr.ElectionStatus()
		if electionStatus != newElectionStatus {
			mgr.Lock()
			mgr.electionStatus = newElectionStatus
			flushManager = mgr.flushManagerWithLock()
			mgr.Unlock()
			flushManager.Init(&mgr.RWMutex, mgr.buckets)
		}

		_, waitFor := flushManager.Flush(&mgr.RWMutex, mgr.buckets)
		if waitFor > 0 {
			mgr.sleepFn(waitFor)
		}
	}
}

func (mgr *flushManager) flushManagerWithLock() roleBasedFlushManager {
	switch mgr.electionStatus {
	case followerStatus:
		return mgr.followerMgr
	case leaderStatus:
		return mgr.leaderMgr
	default:
		// We should never get here.
		panic(fmt.Sprintf("unknown election status %v", mgr.electionStatus))
	}
}

// flushBucket contains all the registered lists for a given flush interval.
type flushBucket struct {
	interval time.Duration
	flushers []PeriodicFlusher
	duration tally.Timer
}

func newBucket(interval time.Duration, scope tally.Scope) *flushBucket {
	return &flushBucket{
		interval: interval,
		duration: scope.Timer("duration"),
	}
}

func (b *flushBucket) Add(flusher PeriodicFlusher) {
	b.flushers = append(b.flushers, flusher)
}
