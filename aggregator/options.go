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
	"sync"
	"time"

	"github.com/m3db/m3aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3aggregator/runtime"
	"github.com/m3db/m3aggregator/sharding"
	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	xtime "github.com/m3db/m3x/time"

	"github.com/spaolacci/murmur3"
)

var (
	defaultMetricPrefix              = []byte("stats.")
	defaultCounterPrefix             = []byte("counts.")
	defaultTimerPrefix               = []byte("timers.")
	defaultGaugePrefix               = []byte("gauges.")
	defaultMinFlushInterval          = 5 * time.Second
	defaultEntryTTL                  = 24 * time.Hour
	defaultEntryCheckInterval        = time.Hour
	defaultEntryCheckBatchPercent    = 0.01
	defaultMaxTimerBatchSizePerWrite = 0
	defaultResignTimeout             = 5 * time.Minute
	defaultDefaultPolicies           = []policy.Policy{
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*24*time.Hour), aggregation.DefaultID),
		policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 40*24*time.Hour), aggregation.DefaultID),
	}

	// By default writes are buffered for 10 minutes before traffic is cut over to a shard
	// in case there are issues with a new instance taking over shards.
	defaultBufferDurationBeforeShardCutover = 10 * time.Minute

	// By default writes are buffered for one hour after traffic is cut off in case there
	// are issues with the instances taking over the shards and as such we need to switch
	// the traffic back to the previous owner of the shards immediately.
	defaultBufferDurationAfterShardCutoff = time.Hour
)

type options struct {
	// Base options.
	aggTypesOptions                  aggregation.TypesOptions
	metricPrefix                     []byte
	counterPrefix                    []byte
	timerPrefix                      []byte
	gaugePrefix                      []byte
	timeLock                         *sync.RWMutex
	clockOpts                        clock.Options
	instrumentOpts                   instrument.Options
	streamOpts                       cm.Options
	runtimeOptsManager               runtime.OptionsManager
	placementManager                 PlacementManager
	shardFn                          sharding.ShardFn
	bufferDurationBeforeShardCutover time.Duration
	bufferDurationAfterShardCutoff   time.Duration
	flushManager                     FlushManager
	minFlushInterval                 time.Duration
	flushHandler                     Handler
	entryTTL                         time.Duration
	entryCheckInterval               time.Duration
	entryCheckBatchPercent           float64
	maxTimerBatchSizePerWrite        int
	defaultPolicies                  []policy.Policy
	flushTimesManager                FlushTimesManager
	electionManager                  ElectionManager
	resignTimeout                    time.Duration
	entryPool                        EntryPool
	counterElemPool                  CounterElemPool
	timerElemPool                    TimerElemPool
	gaugeElemPool                    GaugeElemPool

	// Derived options.
	fullCounterPrefix []byte
	fullTimerPrefix   []byte
	fullGaugePrefix   []byte
	timerQuantiles    []float64
}

// NewOptions create a new set of options.
func NewOptions() Options {
	o := &options{
		aggTypesOptions:    aggregation.NewTypesOptions(),
		metricPrefix:       defaultMetricPrefix,
		counterPrefix:      defaultCounterPrefix,
		timerPrefix:        defaultTimerPrefix,
		gaugePrefix:        defaultGaugePrefix,
		timeLock:           &sync.RWMutex{},
		clockOpts:          clock.NewOptions(),
		instrumentOpts:     instrument.NewOptions(),
		streamOpts:         cm.NewOptions(),
		runtimeOptsManager: runtime.NewOptionsManager(runtime.NewOptions()),
		shardFn:            defaultShardFn,
		bufferDurationBeforeShardCutover: defaultBufferDurationBeforeShardCutover,
		bufferDurationAfterShardCutoff:   defaultBufferDurationAfterShardCutoff,
		minFlushInterval:                 defaultMinFlushInterval,
		entryTTL:                         defaultEntryTTL,
		entryCheckInterval:               defaultEntryCheckInterval,
		entryCheckBatchPercent:           defaultEntryCheckBatchPercent,
		maxTimerBatchSizePerWrite:        defaultMaxTimerBatchSizePerWrite,
		defaultPolicies:                  defaultDefaultPolicies,
		resignTimeout:                    defaultResignTimeout,
	}

	// Initialize pools.
	o.initPools()

	// Compute derived options.
	o.computeAllDerived()

	return o
}

func (o *options) SetMetricPrefix(value []byte) Options {
	opts := *o
	opts.metricPrefix = value
	opts.computeFullPrefixes()
	return &opts
}

func (o *options) MetricPrefix() []byte {
	return o.metricPrefix
}

func (o *options) SetCounterPrefix(value []byte) Options {
	opts := *o
	opts.counterPrefix = value
	opts.computeFullCounterPrefix()
	return &opts
}

func (o *options) CounterPrefix() []byte {
	return o.counterPrefix
}

func (o *options) SetTimerPrefix(value []byte) Options {
	opts := *o
	opts.timerPrefix = value
	opts.computeFullTimerPrefix()
	return &opts
}

func (o *options) TimerPrefix() []byte {
	return o.timerPrefix
}

func (o *options) SetGaugePrefix(value []byte) Options {
	opts := *o
	opts.gaugePrefix = value
	opts.computeFullGaugePrefix()
	return &opts
}

func (o *options) GaugePrefix() []byte {
	return o.gaugePrefix
}

func (o *options) SetTimeLock(value *sync.RWMutex) Options {
	opts := *o
	opts.timeLock = value
	return &opts
}

func (o *options) TimeLock() *sync.RWMutex {
	return o.timeLock
}

func (o *options) SetAggregationTypesOptions(value aggregation.TypesOptions) Options {
	opts := *o
	opts.aggTypesOptions = value
	return &opts
}

func (o *options) AggregationTypesOptions() aggregation.TypesOptions {
	return o.aggTypesOptions
}

func (o *options) SetClockOptions(value clock.Options) Options {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *options) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetStreamOptions(value cm.Options) Options {
	opts := *o
	opts.streamOpts = value
	return &opts
}

func (o *options) StreamOptions() cm.Options {
	return o.streamOpts
}

func (o *options) SetRuntimeOptionsManager(value runtime.OptionsManager) Options {
	opts := *o
	opts.runtimeOptsManager = value
	return &opts
}

func (o *options) RuntimeOptionsManager() runtime.OptionsManager {
	return o.runtimeOptsManager
}

func (o *options) SetPlacementManager(value PlacementManager) Options {
	opts := *o
	opts.placementManager = value
	return &opts
}

func (o *options) PlacementManager() PlacementManager {
	return o.placementManager
}

func (o *options) SetShardFn(value sharding.ShardFn) Options {
	opts := *o
	opts.shardFn = value
	return &opts
}

func (o *options) ShardFn() sharding.ShardFn {
	return o.shardFn
}

func (o *options) SetBufferDurationBeforeShardCutover(value time.Duration) Options {
	opts := *o
	opts.bufferDurationBeforeShardCutover = value
	return &opts
}

func (o *options) BufferDurationBeforeShardCutover() time.Duration {
	return o.bufferDurationBeforeShardCutover
}

func (o *options) SetBufferDurationAfterShardCutoff(value time.Duration) Options {
	opts := *o
	opts.bufferDurationAfterShardCutoff = value
	return &opts
}

func (o *options) BufferDurationAfterShardCutoff() time.Duration {
	return o.bufferDurationAfterShardCutoff
}

func (o *options) SetFlushTimesManager(value FlushTimesManager) Options {
	opts := *o
	opts.flushTimesManager = value
	return &opts
}

func (o *options) FlushTimesManager() FlushTimesManager {
	return o.flushTimesManager
}

func (o *options) SetElectionManager(value ElectionManager) Options {
	opts := *o
	opts.electionManager = value
	return &opts
}

func (o *options) ElectionManager() ElectionManager {
	return o.electionManager
}

func (o *options) SetFlushManager(value FlushManager) Options {
	opts := *o
	opts.flushManager = value
	return &opts
}

func (o *options) FlushManager() FlushManager {
	return o.flushManager
}

func (o *options) SetMinFlushInterval(value time.Duration) Options {
	opts := *o
	opts.minFlushInterval = value
	return &opts
}

func (o *options) MinFlushInterval() time.Duration {
	return o.minFlushInterval
}

func (o *options) SetFlushHandler(value Handler) Options {
	opts := *o
	opts.flushHandler = value
	return &opts
}

func (o *options) FlushHandler() Handler {
	return o.flushHandler
}

func (o *options) SetEntryTTL(value time.Duration) Options {
	opts := *o
	opts.entryTTL = value
	return &opts
}

func (o *options) EntryTTL() time.Duration {
	return o.entryTTL
}

func (o *options) SetEntryCheckInterval(value time.Duration) Options {
	opts := *o
	opts.entryCheckInterval = value
	return &opts
}

func (o *options) EntryCheckInterval() time.Duration {
	return o.entryCheckInterval
}

func (o *options) SetEntryCheckBatchPercent(value float64) Options {
	opts := *o
	opts.entryCheckBatchPercent = value
	return &opts
}

func (o *options) EntryCheckBatchPercent() float64 {
	return o.entryCheckBatchPercent
}

func (o *options) SetMaxTimerBatchSizePerWrite(value int) Options {
	opts := *o
	opts.maxTimerBatchSizePerWrite = value
	return &opts
}

func (o *options) MaxTimerBatchSizePerWrite() int {
	return o.maxTimerBatchSizePerWrite
}

func (o *options) SetDefaultPolicies(value []policy.Policy) Options {
	opts := *o
	opts.defaultPolicies = value
	return &opts
}

func (o *options) DefaultPolicies() []policy.Policy {
	return o.defaultPolicies
}

func (o *options) SetResignTimeout(value time.Duration) Options {
	opts := *o
	opts.resignTimeout = value
	return &opts
}

func (o *options) ResignTimeout() time.Duration {
	return o.resignTimeout
}

func (o *options) SetEntryPool(value EntryPool) Options {
	opts := *o
	opts.entryPool = value
	return &opts
}

func (o *options) EntryPool() EntryPool {
	return o.entryPool
}

func (o *options) SetCounterElemPool(value CounterElemPool) Options {
	opts := *o
	opts.counterElemPool = value
	return &opts
}

func (o *options) CounterElemPool() CounterElemPool {
	return o.counterElemPool
}

func (o *options) SetTimerElemPool(value TimerElemPool) Options {
	opts := *o
	opts.timerElemPool = value
	return &opts
}

func (o *options) TimerElemPool() TimerElemPool {
	return o.timerElemPool
}

func (o *options) SetGaugeElemPool(value GaugeElemPool) Options {
	opts := *o
	opts.gaugeElemPool = value
	return &opts
}

func (o *options) GaugeElemPool() GaugeElemPool {
	return o.gaugeElemPool
}

func (o *options) FullCounterPrefix() []byte {
	return o.fullCounterPrefix
}

func (o *options) FullTimerPrefix() []byte {
	return o.fullTimerPrefix
}

func (o *options) FullGaugePrefix() []byte {
	return o.fullGaugePrefix
}

func (o *options) TimerQuantiles() []float64 {
	return o.timerQuantiles
}

func (o *options) initPools() {
	defaultRuntimeOpts := runtime.NewOptions()
	o.entryPool = NewEntryPool(nil)
	o.entryPool.Init(func() *Entry {
		return NewEntry(nil, defaultRuntimeOpts, o)
	})

	o.counterElemPool = NewCounterElemPool(nil)
	o.counterElemPool.Init(func() *CounterElem {
		return NewCounterElem(nil, policy.EmptyStoragePolicy, aggregation.DefaultTypes, o)
	})

	o.timerElemPool = NewTimerElemPool(nil)
	o.timerElemPool.Init(func() *TimerElem {
		return NewTimerElem(nil, policy.EmptyStoragePolicy, aggregation.DefaultTypes, o)
	})

	o.gaugeElemPool = NewGaugeElemPool(nil)
	o.gaugeElemPool.Init(func() *GaugeElem {
		return NewGaugeElem(nil, policy.EmptyStoragePolicy, aggregation.DefaultTypes, o)
	})
}

func (o *options) computeAllDerived() {
	o.computeFullPrefixes()
}

func (o *options) computeFullPrefixes() {
	o.computeFullCounterPrefix()
	o.computeFullTimerPrefix()
	o.computeFullGaugePrefix()
}

func (o *options) computeFullCounterPrefix() {
	fullCounterPrefix := make([]byte, len(o.metricPrefix)+len(o.counterPrefix))
	n := copy(fullCounterPrefix, o.metricPrefix)
	copy(fullCounterPrefix[n:], o.counterPrefix)
	o.fullCounterPrefix = fullCounterPrefix
}

func (o *options) computeFullTimerPrefix() {
	fullTimerPrefix := make([]byte, len(o.metricPrefix)+len(o.timerPrefix))
	n := copy(fullTimerPrefix, o.metricPrefix)
	copy(fullTimerPrefix[n:], o.timerPrefix)
	o.fullTimerPrefix = fullTimerPrefix
}

func (o *options) computeFullGaugePrefix() {
	fullGaugePrefix := make([]byte, len(o.metricPrefix)+len(o.gaugePrefix))
	n := copy(fullGaugePrefix, o.metricPrefix)
	copy(fullGaugePrefix[n:], o.gaugePrefix)
	o.fullGaugePrefix = fullGaugePrefix
}

func defaultShardFn(id []byte, numShards int) uint32 {
	return murmur3.Sum32(id) % uint32(numShards)
}
