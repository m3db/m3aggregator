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
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"

	"github.com/uber-go/tally"
)

// Aggregator aggregates different types of metrics.
type Aggregator interface {
	// Open opens the aggregator.
	Open() error

	// AddMetricWithPoliciesList adds a metric with policies list for aggregation.
	AddMetricWithPoliciesList(mu unaggregated.MetricUnion, pl policy.PoliciesList) error

	// Resign stops the aggregator from participating in leader election and resigns
	// from ongoing campaign if any.
	Resign() error

	// Status returns the run-time status of the aggregator.
	Status() RuntimeStatus

	// Close closes the aggregator.
	Close() error
}

const (
	uninitializedCutoverNanos = math.MinInt64
)

var (
	errAggregatorNotOpenOrClosed     = errors.New("aggregator is not open or closed")
	errAggregatorAlreadyOpenOrClosed = errors.New("aggregator is already open or closed")
	errInvalidMetricType             = errors.New("invalid metric type")
	errActivePlacementChanged        = errors.New("active placement has changed")
)

type aggregatorTickMetrics struct {
	scope            tally.Scope
	flushTimesErrors tally.Counter
	duration         tally.Timer
	activeEntries    tally.Gauge
	expiredEntries   tally.Counter
	activeElems      map[time.Duration]tally.Gauge
}

func newAggregatorTickMetrics(scope tally.Scope) aggregatorTickMetrics {
	return aggregatorTickMetrics{
		scope:            scope,
		flushTimesErrors: scope.Counter("flush-times-errors"),
		duration:         scope.Timer("duration"),
		activeEntries:    scope.Gauge("active-entries"),
		expiredEntries:   scope.Counter("expired-entries"),
		activeElems:      make(map[time.Duration]tally.Gauge),
	}
}

func (m aggregatorTickMetrics) Report(tickResult tickResult, duration time.Duration) {
	m.duration.Record(duration)
	m.activeEntries.Update(float64(tickResult.ActiveEntries))
	m.expiredEntries.Inc(int64(tickResult.ExpiredEntries))
	for dur, val := range tickResult.ActiveElems {
		gauge, exists := m.activeElems[dur]
		if !exists {
			gauge = m.scope.Tagged(
				map[string]string{"resolution": dur.String()},
			).Gauge("active-elems")
			m.activeElems[dur] = gauge
		}
		gauge.Update(float64(val))
	}
}

type aggregatorShardsMetrics struct {
	add          tally.Counter
	close        tally.Counter
	owned        tally.Gauge
	pendingClose tally.Gauge
}

func newAggregatorShardsMetrics(scope tally.Scope) aggregatorShardsMetrics {
	return aggregatorShardsMetrics{
		add:          scope.Counter("add"),
		close:        scope.Counter("close"),
		owned:        scope.Gauge("owned"),
		pendingClose: scope.Gauge("pending-close"),
	}
}

type aggregatorMetrics struct {
	counters                  tally.Counter
	timers                    tally.Counter
	timerBatches              tally.Counter
	gauges                    tally.Counter
	invalidMetricTypes        tally.Counter
	addMetricWithPoliciesList instrument.MethodMetrics
	shards                    aggregatorShardsMetrics
	tick                      aggregatorTickMetrics
}

func newAggregatorMetrics(scope tally.Scope, samplingRate float64) aggregatorMetrics {
	shardsScope := scope.SubScope("shards")
	tickScope := scope.SubScope("tick")
	return aggregatorMetrics{
		counters:                  scope.Counter("counters"),
		timers:                    scope.Counter("timers"),
		timerBatches:              scope.Counter("timer-batches"),
		gauges:                    scope.Counter("gauges"),
		invalidMetricTypes:        scope.Counter("invalid-metric-types"),
		addMetricWithPoliciesList: instrument.NewMethodMetrics(scope, "addMetricWithPoliciesList", samplingRate),
		shards: newAggregatorShardsMetrics(shardsScope),
		tick:   newAggregatorTickMetrics(tickScope),
	}
}

// RuntimeStatus contains run-time status of the aggregator.
type RuntimeStatus struct {
	FlushStatus FlushStatus `json:"flushStatus"`
}

type updateShardsType int

const (
	noUpdateShards updateShardsType = iota
	updateShards
)

type aggregatorState int

const (
	aggregatorNotOpen aggregatorState = iota
	aggregatorOpen
	aggregatorClosed
)

type sleepFn func(d time.Duration)

// aggregator stores aggregations of different types of metrics (e.g., counter,
// timer, gauges) and periodically flushes them out.
type aggregator struct {
	sync.RWMutex

	opts              Options
	nowFn             clock.NowFn
	shardFn           ShardFn
	checkInterval     time.Duration
	placementManager  PlacementManager
	flushTimesManager FlushTimesManager
	flushTimesChecker flushTimesChecker
	electionManager   ElectionManager
	flushManager      FlushManager
	flushHandler      Handler
	resignTimeout     time.Duration

	shardSetID            uint32
	shardSetOpen          bool
	shardIDs              []uint32
	shards                []*aggregatorShard
	placementCutoverNanos int64
	state                 aggregatorState
	doneCh                chan struct{}
	wg                    sync.WaitGroup
	sleepFn               sleepFn
	shardsPendingClose    int32
	metrics               aggregatorMetrics
}

// NewAggregator creates a new aggregator.
func NewAggregator(opts Options) Aggregator {
	iOpts := opts.InstrumentOptions()
	scope := iOpts.MetricsScope()
	metrics := newAggregatorMetrics(scope, iOpts.MetricsSamplingRate())

	return &aggregator{
		opts:                  opts,
		nowFn:                 opts.ClockOptions().NowFn(),
		shardFn:               opts.ShardFn(),
		checkInterval:         opts.EntryCheckInterval(),
		placementManager:      opts.PlacementManager(),
		flushTimesManager:     opts.FlushTimesManager(),
		flushTimesChecker:     newFlushTimesChecker(scope.SubScope("tick.shard-check")),
		electionManager:       opts.ElectionManager(),
		flushManager:          opts.FlushManager(),
		flushHandler:          opts.FlushHandler(),
		resignTimeout:         opts.ResignTimeout(),
		placementCutoverNanos: uninitializedCutoverNanos,
		metrics:               metrics,
		doneCh:                make(chan struct{}),
		sleepFn:               time.Sleep,
	}
}

func (agg *aggregator) Open() error {
	agg.Lock()
	defer agg.Unlock()

	if agg.state != aggregatorNotOpen {
		return errAggregatorAlreadyOpenOrClosed
	}
	if err := agg.placementManager.Open(); err != nil {
		return err
	}
	placement, err := agg.placementManager.Placement()
	if err != nil {
		return err
	}
	if err := agg.processPlacementWithLock(placement); err != nil {
		return err
	}
	if agg.checkInterval > 0 {
		agg.wg.Add(1)
		go agg.tick()
	}
	agg.state = aggregatorOpen
	return nil
}

func (agg *aggregator) AddMetricWithPoliciesList(
	mu unaggregated.MetricUnion,
	pl policy.PoliciesList,
) error {
	callStart := agg.nowFn()
	if err := agg.checkMetricType(mu); err != nil {
		agg.metrics.addMetricWithPoliciesList.ReportError(agg.nowFn().Sub(callStart))
		return err
	}
	shard, err := agg.shardFor(mu.ID)
	if err != nil {
		agg.metrics.addMetricWithPoliciesList.ReportError(agg.nowFn().Sub(callStart))
		return err
	}
	err = shard.AddMetricWithPoliciesList(mu, pl)
	agg.metrics.addMetricWithPoliciesList.ReportSuccessOrError(err, agg.nowFn().Sub(callStart))
	return err
}

func (agg *aggregator) Resign() error {
	ctx, cancel := context.WithTimeout(context.Background(), agg.resignTimeout)
	defer cancel()
	return agg.electionManager.Resign(ctx)
}

func (agg *aggregator) Status() RuntimeStatus {
	return RuntimeStatus{
		FlushStatus: agg.flushManager.Status(),
	}
}

func (agg *aggregator) Close() error {
	agg.Lock()
	defer agg.Unlock()

	if agg.state != aggregatorOpen {
		return errAggregatorNotOpenOrClosed
	}
	close(agg.doneCh)
	for _, shardID := range agg.shardIDs {
		agg.shards[shardID].Close()
	}
	if agg.shardSetOpen {
		agg.closeShardSetWithLock()
	}
	agg.flushHandler.Close()
	agg.state = aggregatorClosed
	return nil
}

func (agg *aggregator) shardFor(id id.RawID) (*aggregatorShard, error) {
	agg.RLock()
	shard, err := agg.shardForWithLock(id, noUpdateShards)
	if err == nil || err != errActivePlacementChanged {
		agg.RUnlock()
		return shard, err
	}
	agg.RUnlock()

	agg.Lock()
	shard, err = agg.shardForWithLock(id, updateShards)
	agg.Unlock()

	return shard, err
}

func (agg *aggregator) shardForWithLock(id id.RawID, updateShardsType updateShardsType) (*aggregatorShard, error) {
	if agg.state != aggregatorOpen {
		return nil, errAggregatorNotOpenOrClosed
	}
	placement, err := agg.placementManager.Placement()
	if err != nil {
		return nil, err
	}
	if agg.shouldProcessPlacementWithLock(placement) {
		if updateShardsType == noUpdateShards {
			return nil, errActivePlacementChanged
		}
		if err := agg.processPlacementWithLock(placement); err != nil {
			return nil, err
		}
	}
	numShards := placement.NumShards()
	shardID := agg.shardFn([]byte(id), numShards)
	if int(shardID) >= len(agg.shards) || agg.shards[shardID] == nil {
		return nil, fmt.Errorf("not responsible for shard %d", shardID)
	}
	return agg.shards[shardID], nil
}

func (agg *aggregator) processPlacementWithLock(newPlacement placement.Placement) error {
	// If someone has already processed the placement ahead of us, do nothing.
	if !agg.shouldProcessPlacementWithLock(newPlacement) {
		return nil
	}
	var newShardSet shard.Shards
	instance, err := agg.placementManager.InstanceFrom(newPlacement)
	if err == nil {
		newShardSet = instance.Shards()
	} else if err == errInstanceNotFoundInPlacement {
		newShardSet = shard.NewShards(nil)
	} else {
		return err
	}
	if err := agg.updateShardSetIDWithLock(instance); err != nil {
		return err
	}
	agg.updateShardsWithLock(newPlacement, newShardSet)
	return nil
}

func (agg *aggregator) shouldProcessPlacementWithLock(newPlacement placement.Placement) bool {
	return agg.placementCutoverNanos < newPlacement.CutoverNanos()
}

// updateShardSetWithLock resets the instance's shard set id given the instance from
// the latest placement, or clears it if the instance is nil (i.e., instance not found).
func (agg *aggregator) updateShardSetIDWithLock(instance placement.Instance) error {
	if instance == nil {
		return agg.clearShardSetIDWithLock()
	}
	return agg.resetShardSetIDWithLock(instance)
}

// clearShardSetIDWithLock clears the instance's shard set id.
func (agg *aggregator) clearShardSetIDWithLock() error {
	if !agg.shardSetOpen {
		return nil
	}
	if err := agg.closeShardSetWithLock(); err != nil {
		return err
	}
	agg.shardSetID = 0
	agg.shardSetOpen = false
	return nil
}

// resetShardSetIDWithLock resets the instance's shard set id given the instance from
// the latest placement.
func (agg *aggregator) resetShardSetIDWithLock(instance placement.Instance) error {
	if !agg.shardSetOpen {
		shardSetID := instance.ShardSetID()
		if err := agg.openShardSetWithLock(shardSetID); err != nil {
			return err
		}
		agg.shardSetID = shardSetID
		agg.shardSetOpen = true
		return nil
	}
	if instance.ShardSetID() == agg.shardSetID {
		return nil
	}
	if err := agg.closeShardSetWithLock(); err != nil {
		return err
	}
	newShardSetID := instance.ShardSetID()
	if err := agg.openShardSetWithLock(newShardSetID); err != nil {
		return err
	}
	agg.shardSetID = newShardSetID
	agg.shardSetOpen = true
	return nil
}

func (agg *aggregator) openShardSetWithLock(shardSetID uint32) error {
	if err := agg.flushTimesManager.Open(shardSetID); err != nil {
		return err
	}
	if err := agg.electionManager.Open(shardSetID); err != nil {
		return err
	}
	return agg.flushManager.Open()
}

func (agg *aggregator) closeShardSetWithLock() error {
	if err := agg.flushManager.Close(); err != nil {
		return err
	}
	if err := agg.flushManager.Reset(); err != nil {
		return err
	}
	if err := agg.electionManager.Close(); err != nil {
		return err
	}
	if err := agg.electionManager.Reset(); err != nil {
		return err
	}
	if err := agg.flushTimesManager.Close(); err != nil {
		return err
	}
	return agg.flushTimesManager.Reset()
}

func (agg *aggregator) updateShardsWithLock(
	newPlacement placement.Placement,
	newShardSet shard.Shards,
) {
	var (
		incoming []*aggregatorShard
		closing  = make([]*aggregatorShard, 0, len(agg.shardIDs))
	)
	for _, shard := range agg.shards {
		if shard == nil {
			continue
		}
		if !newShardSet.Contains(shard.ID()) {
			closing = append(closing, shard)
		}
	}

	// NB(xichen): shards are guaranteed to be sorted by their ids in ascending order.
	var (
		newShards   = newShardSet.All()
		newShardIDs []uint32
	)
	if numShards := len(newShards); numShards > 0 {
		newShardIDs = make([]uint32, 0, numShards)
		maxShardID := newShards[numShards-1].ID()
		incoming = make([]*aggregatorShard, maxShardID+1)
	}
	for _, shard := range newShards {
		shardID := shard.ID()
		newShardIDs = append(newShardIDs, shardID)
		if int(shardID) < len(agg.shards) && agg.shards[shardID] != nil {
			incoming[shardID] = agg.shards[shardID]
		} else {
			incoming[shardID] = newAggregatorShard(shardID, agg.opts)
			agg.metrics.shards.add.Inc(1)
		}
		shardTimeRange := timeRange{
			cutoverNanos: shard.CutoverNanos(),
			cutoffNanos:  shard.CutoffNanos(),
		}
		incoming[shardID].SetWriteableRange(shardTimeRange)
	}

	agg.shardIDs = newShardIDs
	agg.shards = incoming
	agg.placementCutoverNanos = newPlacement.CutoverNanos()
	agg.closeShardsAsync(closing)
}

func (agg *aggregator) checkMetricType(mu unaggregated.MetricUnion) error {
	switch mu.Type {
	case unaggregated.CounterType:
		agg.metrics.counters.Inc(1)
		return nil
	case unaggregated.BatchTimerType:
		agg.metrics.timerBatches.Inc(1)
		agg.metrics.timers.Inc(int64(len(mu.BatchTimerVal)))
		return nil
	case unaggregated.GaugeType:
		agg.metrics.gauges.Inc(1)
		return nil
	default:
		agg.metrics.invalidMetricTypes.Inc(1)
		return errInvalidMetricType
	}
}

func (agg *aggregator) ownedShards() (owned, toClose []*aggregatorShard) {
	agg.Lock()
	defer agg.Unlock()

	flushTimes, err := agg.flushTimesManager.Get()
	if err != nil {
		agg.metrics.tick.flushTimesErrors.Inc(1)
	}
	owned = make([]*aggregatorShard, 0, len(agg.shardIDs))
	for i := 0; i < len(agg.shardIDs); i++ {
		shardID := agg.shardIDs[i]
		shard := agg.shards[shardID]
		// NB(xichen): a shard can be closed when all of the following conditions are met:
		// * The shard is not writeable.
		// * The shard has been cut off (we do not want to close a shard that has not been
		//   cut over in that it may be warming up).
		// * All of the shard's data has been flushed up until the shard's cutoff time.
		canCloseShard := !shard.IsWritable() &&
			shard.IsCutoff() &&
			agg.flushTimesChecker.HasFlushed(
				shard.ID(),
				shard.CutoffNanos(),
				flushTimes,
			)
		if !canCloseShard {
			owned = append(owned, shard)
		} else {
			lastIdx := len(agg.shardIDs) - 1
			agg.shardIDs[i], agg.shardIDs[lastIdx] = agg.shardIDs[lastIdx], agg.shardIDs[i]
			agg.shardIDs = agg.shardIDs[:lastIdx]
			i--
			agg.shards[shardID] = nil
			toClose = append(toClose, shard)
		}
	}
	return owned, toClose
}

// closeShardsAsync asynchronously closes the shards to avoid blocking writes.
// Because each shard write happens while holding the shard read lock, the shard
// may only close itself after all its pending writes are finished.
func (agg *aggregator) closeShardsAsync(shards []*aggregatorShard) {
	pendingClose := atomic.AddInt32(&agg.shardsPendingClose, int32(len(shards)))
	agg.metrics.shards.pendingClose.Update(float64(pendingClose))

	for _, shard := range shards {
		shard := shard
		go func() {
			shard.Close()
			pendingClose := atomic.AddInt32(&agg.shardsPendingClose, -1)
			agg.metrics.shards.pendingClose.Update(float64(pendingClose))
			agg.metrics.shards.close.Inc(1)
		}()
	}
}

func (agg *aggregator) tick() {
	defer agg.wg.Done()

	for {
		select {
		case <-agg.doneCh:
			return
		default:
			agg.tickInternal()
		}
	}
}

func (agg *aggregator) tickInternal() {
	ownedShards, closingShards := agg.ownedShards()
	agg.closeShardsAsync(closingShards)

	numShards := len(ownedShards)
	agg.metrics.shards.owned.Update(float64(numShards))
	agg.metrics.shards.pendingClose.Update(float64(atomic.LoadInt32(&agg.shardsPendingClose)))
	if numShards == 0 {
		return
	}
	var (
		start                = agg.nowFn()
		perShardTickDuration = agg.checkInterval / time.Duration(numShards)
		tickResult           tickResult
	)
	for _, shard := range ownedShards {
		shardTickResult := shard.Tick(perShardTickDuration)
		tickResult = tickResult.merge(shardTickResult)
	}
	tickDuration := agg.nowFn().Sub(start)
	agg.metrics.tick.Report(tickResult, tickDuration)
	if tickDuration < agg.checkInterval {
		agg.sleepFn(agg.checkInterval - tickDuration)
	}
}
