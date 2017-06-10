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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/m3db/m3aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/time"

	"github.com/spaolacci/murmur3"
)

var (
	defaultMetricPrefix              = []byte("stats.")
	defaultCounterPrefix             = []byte("counts.")
	defaultTimerPrefix               = []byte("timers.")
	defaultGaugePrefix               = []byte("gauges.")
	defaultAggregationLastSuffix     = []byte(".last")
	defaultAggregationSumSuffix      = []byte(".sum")
	defaultAggregationSumSqSuffix    = []byte(".sum_sq")
	defaultAggregationMeanSuffix     = []byte(".mean")
	defaultAggregationLowerSuffix    = []byte(".lower")
	defaultAggregationUpperSuffix    = []byte(".upper")
	defaultAggregationCountSuffix    = []byte(".count")
	defaultAggregationStdevSuffix    = []byte(".stdev")
	defaultAggregationMedianSuffix   = []byte(".median")
	defaultInstanceID                = "localhost"
	defaultMinFlushInterval          = 5 * time.Second
	defaultMaxFlushSize              = 1440
	defaultEntryTTL                  = 24 * time.Hour
	defaultEntryCheckInterval        = time.Hour
	defaultEntryCheckBatchPercent    = 0.01
	defaultMaxTimerBatchSizePerWrite = 0
	defaultDefaultPolicies           = []policy.Policy{
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*24*time.Hour), policy.DefaultAggregationID),
		policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 40*24*time.Hour), policy.DefaultAggregationID),
	}

	defaultTimerAggregationTypes = policy.AggregationTypes{
		policy.Sum,
		policy.SumSq,
		policy.Mean,
		policy.Lower,
		policy.Upper,
		policy.Count,
		policy.Stdev,
		policy.Median,
		policy.P50,
		policy.P95,
		policy.P99,
	}

	defaultQuantilePrecision = 0.0000000001 // 9 zeros after 0.
)

type options struct {
	// Base options.
	metricPrefix              []byte
	counterPrefix             []byte
	timerPrefix               []byte
	sumSuffix                 []byte
	sumSqSuffix               []byte
	meanSuffix                []byte
	lastSuffix                []byte
	lowerSuffix               []byte
	upperSuffix               []byte
	countSuffix               []byte
	stdevSuffix               []byte
	medianSuffix              []byte
	timerAggTypes             policy.AggregationTypes
	timerQuantiles            []float64
	timerQuantileSuffixFn     QuantileSuffixFn
	gaugePrefix               []byte
	timeLock                  *sync.RWMutex
	clockOpts                 clock.Options
	instrumentOpts            instrument.Options
	streamOpts                cm.Options
	placementWatcherOpts      services.StagedPlacementWatcherOptions
	instanceID                string
	shardFn                   ShardFn
	flushManager              FlushManager
	minFlushInterval          time.Duration
	maxFlushSize              int
	flushHandler              Handler
	entryTTL                  time.Duration
	entryCheckInterval        time.Duration
	entryCheckBatchPercent    float64
	maxTimerBatchSizePerWrite int
	defaultPolicies           []policy.Policy
	entryPool                 EntryPool
	counterElemPool           CounterElemPool
	timerElemPool             TimerElemPool
	gaugeElemPool             GaugeElemPool
	bufferedEncoderPool       msgpack.BufferedEncoderPool
	aggTypesPool              policy.AggregationTypesPool
	quantilesPool             pool.FloatsPool

	// Derived options.
	fullCounterPrefix []byte
	fullTimerPrefix   []byte
	fullGaugePrefix   []byte
	suffixMap         map[policy.AggregationType][]byte
}

// NewOptions create a new set of options.
func NewOptions() Options {
	o := &options{
		metricPrefix:              defaultMetricPrefix,
		counterPrefix:             defaultCounterPrefix,
		timerPrefix:               defaultTimerPrefix,
		lastSuffix:                defaultAggregationLastSuffix,
		sumSuffix:                 defaultAggregationSumSuffix,
		sumSqSuffix:               defaultAggregationSumSqSuffix,
		meanSuffix:                defaultAggregationMeanSuffix,
		lowerSuffix:               defaultAggregationLowerSuffix,
		upperSuffix:               defaultAggregationUpperSuffix,
		countSuffix:               defaultAggregationCountSuffix,
		stdevSuffix:               defaultAggregationStdevSuffix,
		medianSuffix:              defaultAggregationMedianSuffix,
		timerAggTypes:             defaultTimerAggregationTypes,
		timerQuantileSuffixFn:     defaultTimerQuantileSuffixFn,
		gaugePrefix:               defaultGaugePrefix,
		timeLock:                  &sync.RWMutex{},
		clockOpts:                 clock.NewOptions(),
		instrumentOpts:            instrument.NewOptions(),
		streamOpts:                cm.NewOptions(),
		placementWatcherOpts:      placement.NewStagedPlacementWatcherOptions(),
		instanceID:                defaultInstanceID,
		shardFn:                   defaultShardFn,
		minFlushInterval:          defaultMinFlushInterval,
		maxFlushSize:              defaultMaxFlushSize,
		entryTTL:                  defaultEntryTTL,
		entryCheckInterval:        defaultEntryCheckInterval,
		entryCheckBatchPercent:    defaultEntryCheckBatchPercent,
		maxTimerBatchSizePerWrite: defaultMaxTimerBatchSizePerWrite,
		defaultPolicies:           defaultDefaultPolicies,
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

func (o *options) SetAggregationLastSuffix(value []byte) Options {
	opts := *o
	opts.lastSuffix = value
	return &opts
}

func (o *options) AggregationLastSuffix() []byte {
	return o.lastSuffix
}

func (o *options) SetAggregationSumSuffix(value []byte) Options {
	opts := *o
	opts.sumSuffix = value
	return &opts
}

func (o *options) AggregationSumSuffix() []byte {
	return o.sumSuffix
}

func (o *options) SetAggregationSumSqSuffix(value []byte) Options {
	opts := *o
	opts.sumSqSuffix = value
	return &opts
}

func (o *options) AggregationSumSqSuffix() []byte {
	return o.sumSqSuffix
}

func (o *options) SetAggregationMeanSuffix(value []byte) Options {
	opts := *o
	opts.meanSuffix = value
	return &opts
}

func (o *options) AggregationMeanSuffix() []byte {
	return o.meanSuffix
}

func (o *options) SetAggregationLowerSuffix(value []byte) Options {
	opts := *o
	opts.lowerSuffix = value
	return &opts
}

func (o *options) AggregationLowerSuffix() []byte {
	return o.lowerSuffix
}

func (o *options) SetAggregationUpperSuffix(value []byte) Options {
	opts := *o
	opts.upperSuffix = value
	return &opts
}

func (o *options) AggregationUpperSuffix() []byte {
	return o.upperSuffix
}

func (o *options) SetAggregationCountSuffix(value []byte) Options {
	opts := *o
	opts.countSuffix = value
	return &opts
}

func (o *options) AggregationCountSuffix() []byte {
	return o.countSuffix
}

func (o *options) SetAggregationStdevSuffix(value []byte) Options {
	opts := *o
	opts.stdevSuffix = value
	return &opts
}

func (o *options) AggregationStdevSuffix() []byte {
	return o.stdevSuffix
}

func (o *options) SetAggregationMedianSuffix(value []byte) Options {
	opts := *o
	opts.medianSuffix = value
	return &opts
}

func (o *options) AggregationMedianSuffix() []byte {
	return o.medianSuffix
}

func (o *options) SetDefaultTimerAggregationTypes(aggTypes policy.AggregationTypes) Options {
	opts := *o
	opts.timerAggTypes = aggTypes
	opts.computeQuantiles()
	return &opts
}

func (o *options) DefaultTimerAggregationTypes() policy.AggregationTypes {
	return o.timerAggTypes
}

func (o *options) SetTimerQuantileSuffixFn(value QuantileSuffixFn) Options {
	opts := *o
	opts.timerQuantileSuffixFn = value
	opts.computeSuffixes()
	return &opts
}

func (o *options) TimerQuantileSuffixFn() QuantileSuffixFn {
	return o.timerQuantileSuffixFn
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

func (o *options) SetStagedPlacementWatcherOptions(value services.StagedPlacementWatcherOptions) Options {
	opts := *o
	opts.placementWatcherOpts = value
	return &opts
}

func (o *options) StagedPlacementWatcherOptions() services.StagedPlacementWatcherOptions {
	return o.placementWatcherOpts
}

func (o *options) SetInstanceID(value string) Options {
	opts := *o
	opts.instanceID = value
	return &opts
}

func (o *options) InstanceID() string {
	return o.instanceID
}

func (o *options) SetShardFn(value ShardFn) Options {
	opts := *o
	opts.shardFn = value
	return &opts
}

func (o *options) ShardFn() ShardFn {
	return o.shardFn
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

func (o *options) SetMaxFlushSize(value int) Options {
	opts := *o
	opts.maxFlushSize = value
	return &opts
}

func (o *options) MaxFlushSize() int {
	return o.maxFlushSize
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

func (o *options) SetBufferedEncoderPool(value msgpack.BufferedEncoderPool) Options {
	opts := *o
	opts.bufferedEncoderPool = value
	return &opts
}

func (o *options) BufferedEncoderPool() msgpack.BufferedEncoderPool {
	return o.bufferedEncoderPool
}

func (o *options) SetAggregationTypesPool(pool policy.AggregationTypesPool) Options {
	opts := *o
	opts.aggTypesPool = pool
	return &opts
}

func (o *options) AggregationTypesPool() policy.AggregationTypesPool {
	return o.aggTypesPool
}

func (o *options) SetQuantilesPool(pool pool.FloatsPool) Options {
	opts := *o
	opts.quantilesPool = pool
	return &opts
}

func (o *options) QuantilesPool() pool.FloatsPool {
	return o.quantilesPool
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

func (o *options) Suffix(aggtype policy.AggregationType) []byte {
	return o.suffixMap[aggtype]
}

func (o *options) initPools() {
	o.entryPool = NewEntryPool(nil)
	o.entryPool.Init(func() *Entry {
		return NewEntry(nil, o)
	})

	o.aggTypesPool = policy.NewAggregationTypesPool(nil)
	o.aggTypesPool.Init(func() policy.AggregationTypes {
		return make(policy.AggregationTypes, 0, len(policy.ValidAggregationTypes))
	})

	o.counterElemPool = NewCounterElemPool(nil)
	o.counterElemPool.Init(func() *CounterElem {
		return NewCounterElem(nil, policy.DefaultStoragePolicy, policy.DefaultAggregationTypes, o)
	})

	o.timerElemPool = NewTimerElemPool(nil)
	o.timerElemPool.Init(func() *TimerElem {
		return NewTimerElem(nil, policy.DefaultStoragePolicy, policy.DefaultAggregationTypes, o)
	})

	o.gaugeElemPool = NewGaugeElemPool(nil)
	o.gaugeElemPool.Init(func() *GaugeElem {
		return NewGaugeElem(nil, policy.DefaultStoragePolicy, policy.DefaultAggregationTypes, o)
	})

	o.bufferedEncoderPool = msgpack.NewBufferedEncoderPool(nil)
	o.bufferedEncoderPool.Init(func() msgpack.BufferedEncoder {
		return msgpack.NewPooledBufferedEncoder(o.bufferedEncoderPool)
	})

	o.quantilesPool = pool.NewFloatsPool(nil, nil)
	o.quantilesPool.Init()
}

func (o *options) computeAllDerived() {
	o.computeFullPrefixes()
	o.computeQuantiles()
	o.computeSuffixes()
}

func (o *options) computeFullPrefixes() {
	o.computeFullCounterPrefix()
	o.computeFullTimerPrefix()
	o.computeFullGaugePrefix()
}

func (o *options) computeQuantiles() {
	o.timerQuantiles, _ = o.DefaultTimerAggregationTypes().PooledQuantiles(o.QuantilesPool())
}

func (o *options) computeSuffixes() {
	o.suffixMap = make(map[policy.AggregationType][]byte, len(policy.ValidAggregationTypes))

	for aggType := range policy.ValidAggregationTypes {
		switch aggType {
		case policy.Last:
			o.suffixMap[policy.Last] = o.AggregationLastSuffix()
		case policy.Lower:
			o.suffixMap[policy.Lower] = o.AggregationLowerSuffix()
		case policy.Upper:
			o.suffixMap[policy.Upper] = o.AggregationUpperSuffix()
		case policy.Mean:
			o.suffixMap[policy.Mean] = o.AggregationMeanSuffix()
		case policy.Median:
			o.suffixMap[policy.Median] = o.AggregationMedianSuffix()
		case policy.Count:
			o.suffixMap[policy.Count] = o.AggregationCountSuffix()
		case policy.Sum:
			o.suffixMap[policy.Sum] = o.AggregationSumSuffix()
		case policy.SumSq:
			o.suffixMap[policy.SumSq] = o.AggregationSumSqSuffix()
		case policy.Stdev:
			o.suffixMap[policy.Stdev] = o.AggregationStdevSuffix()
		default:
			q, ok := aggType.Quantile()
			if ok {
				o.suffixMap[aggType] = o.timerQuantileSuffixFn(q)
			}
		}
	}
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

// By default we use e.g. ".p50", ".p95", ".p99" for the 50th/95th/99th percentile.
func defaultTimerQuantileSuffixFn(quantile float64) []byte {
	s := fmt.Sprintf("%0.0f", quantile/defaultQuantilePrecision)
	return []byte(fmt.Sprintf(".p%s", trimQuantileSuffix(s)))
}

func trimQuantileSuffix(s string) string {
	for strings.HasSuffix(s, "0") && len(s) > 2 {
		s = strings.TrimSuffix(s, "0")
	}
	return s
}

func defaultShardFn(id []byte, numShards int) uint32 {
	return murmur3.Sum32(id) % uint32(numShards)
}
