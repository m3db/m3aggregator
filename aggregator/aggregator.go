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
	"time"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
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
	scope          tally.Scope
	duration       tally.Timer
	activeEntries  tally.Gauge
	expiredEntries tally.Counter
	activeElems    map[time.Duration]tally.Gauge
}

func newAggregatorTickMetrics(scope tally.Scope) aggregatorTickMetrics {
	return aggregatorTickMetrics{
		scope:          scope,
		duration:       scope.Timer("duration"),
		activeEntries:  scope.Gauge("active-entries"),
		expiredEntries: scope.Counter("expired-entries"),
		activeElems:    make(map[time.Duration]tally.Gauge),
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

type aggregatorShardMetrics struct {
	add   tally.Counter
	close tally.Counter
	owned tally.Gauge
}

func newAggregatorShardMetrics(scope tally.Scope) aggregatorShardMetrics {
	return aggregatorShardMetrics{
		add:   scope.Counter("add"),
		close: scope.Counter("close"),
		owned: scope.Gauge("owned"),
	}
}

type aggregatorMetrics struct {
	counters                  tally.Counter
	timers                    tally.Counter
	timerBatches              tally.Counter
	gauges                    tally.Counter
	invalidMetricTypes        tally.Counter
	addMetricWithPoliciesList instrument.MethodMetrics
	shards                    aggregatorShardMetrics
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
		shards: newAggregatorShardMetrics(shardsScope),
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

	opts            Options
	nowFn           clock.NowFn
	instanceID      string
	shardFn         ShardFn
	checkInterval   time.Duration
	electionManager ElectionManager
	flushManager    FlushManager
	flushHandler    Handler
	resignTimeout   time.Duration

	shardIDs              []uint32
	shards                []*aggregatorShard
	placementWatcher      services.StagedPlacementWatcher
	placementCutoverNanos int64
	state                 aggregatorState
	doneCh                chan struct{}
	wg                    sync.WaitGroup
	sleepFn               sleepFn
	metrics               aggregatorMetrics
}

// NewAggregator creates a new aggregator.
func NewAggregator(opts Options) Aggregator {
	placementWatcherOpts := opts.StagedPlacementWatcherOptions()
	placementWatcher := placement.NewStagedPlacementWatcher(placementWatcherOpts)
	iOpts := opts.InstrumentOptions()
	metrics := newAggregatorMetrics(iOpts.MetricsScope(), iOpts.MetricsSamplingRate())

	return &aggregator{
		opts:                  opts,
		nowFn:                 opts.ClockOptions().NowFn(),
		instanceID:            opts.InstanceID(),
		shardFn:               opts.ShardFn(),
		checkInterval:         opts.EntryCheckInterval(),
		electionManager:       opts.ElectionManager(),
		flushManager:          opts.FlushManager(),
		flushHandler:          opts.FlushHandler(),
		resignTimeout:         opts.ResignTimeout(),
		placementWatcher:      placementWatcher,
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
	if err := agg.placementWatcher.Watch(); err != nil {
		return err
	}
	placement, err := agg.placement()
	if err != nil {
		return err
	}
	if err := agg.updateShardsWithLock(placement); err != nil {
		return err
	}
	instance, err := agg.instance(placement)
	if err != nil {
		return err
	}
	shardSetID := instance.ShardSetID()
	if err := agg.electionManager.Open(shardSetID); err != nil {
		return err
	}
	if err := agg.flushManager.Open(shardSetID); err != nil {
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
	agg.state = aggregatorClosed
	close(agg.doneCh)
	for _, shardID := range agg.shardIDs {
		agg.shards[shardID].Close()
	}
	agg.electionManager.Close()
	agg.flushManager.Close()
	agg.flushHandler.Close()
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
	placement, err := agg.placement()
	if err != nil {
		return nil, err
	}
	return agg.findOrUpdateShardWithLock(id, placement, updateShardsType)
}

func (agg *aggregator) findOrUpdateShardWithLock(
	id id.RawID,
	placement services.Placement,
	updateShardsType updateShardsType,
) (*aggregatorShard, error) {
	if agg.shouldUpdateShardsWithLock(placement) {
		if updateShardsType == noUpdateShards {
			return nil, errActivePlacementChanged
		}
		if err := agg.updateShardsWithLock(placement); err != nil {
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

func (agg *aggregator) initShardsWithLock() error {
	placement, err := agg.placement()
	if err != nil {
		return err
	}
	return agg.updateShardsWithLock(placement)
}

func (agg *aggregator) updateShardsWithLock(newPlacement services.Placement) error {
	// If someone has already updated the shards ahead of us, do nothing.
	if !agg.shouldUpdateShardsWithLock(newPlacement) {
		return nil
	}
	instance, err := agg.instance(newPlacement)
	if err != nil {
		return err
	}

	var (
		newShards = instance.Shards()
		incoming  []*aggregatorShard
		closing   []*aggregatorShard
	)
	for _, shard := range agg.shards {
		if shard == nil {
			continue
		}
		if !newShards.Contains(shard.ID()) {
			closing = append(closing, shard)
		}
	}
	// NB(xichen): shard ids are guaranteed to be sorted in ascending order.
	newShardIDs := newShards.AllIDs()
	if len(newShardIDs) > 0 {
		newShardLen := newShardIDs[len(newShardIDs)-1] + 1
		incoming = make([]*aggregatorShard, newShardLen)
	}
	for _, shardID := range newShardIDs {
		if int(shardID) < len(agg.shards) && agg.shards[shardID] != nil {
			incoming[shardID] = agg.shards[shardID]
		} else {
			incoming[shardID] = newAggregatorShard(shardID, agg.opts)
			agg.metrics.shards.add.Inc(1)
		}
	}
	agg.shardIDs = newShardIDs
	agg.shards = incoming
	agg.placementCutoverNanos = newPlacement.CutoverNanos()

	// NB(xichen): asynchronously closing the shards to avoid blocking writes.
	// Additionally, because each shard write happens while holding the shard
	// read lock, the shard may only close itself after all its pending writes
	// are finished.
	for _, shard := range closing {
		shard := shard
		go func() {
			shard.Close()
			agg.metrics.shards.close.Inc(1)
		}()
	}

	return nil
}

func (agg *aggregator) shouldUpdateShardsWithLock(newPlacement services.Placement) bool {
	return agg.placementCutoverNanos < newPlacement.CutoverNanos()
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

func (agg *aggregator) ownedShards() []*aggregatorShard {
	agg.RLock()
	ownedShards := make([]*aggregatorShard, len(agg.shardIDs))
	for i, shardID := range agg.shardIDs {
		ownedShards[i] = agg.shards[shardID]
	}
	agg.RUnlock()
	return ownedShards
}

func (agg *aggregator) placement() (services.Placement, error) {
	stagedPlacement, onStagedPlacementDoneFn, err := agg.placementWatcher.ActiveStagedPlacement()
	if err != nil {
		return nil, err
	}
	placement, onPlacementDoneFn, err := stagedPlacement.ActivePlacement()
	if err != nil {
		onStagedPlacementDoneFn()
		return nil, err
	}
	onPlacementDoneFn()
	onStagedPlacementDoneFn()
	return placement, nil
}

func (agg *aggregator) instance(placement services.Placement) (services.PlacementInstance, error) {
	instance, found := placement.Instance(agg.instanceID)
	if !found {
		return nil, fmt.Errorf("instance %s not found in the placement", agg.instanceID)
	}
	return instance, nil
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
	ownedShards := agg.ownedShards()
	numShards := len(ownedShards)
	agg.metrics.shards.owned.Update(float64(numShards))
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
