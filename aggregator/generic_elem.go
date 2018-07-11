// Copyright (c) 2018 Uber Technologies, Inc.
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
	"math"
	"sync"
	"time"

	raggregation "github.com/m3db/m3aggregator/aggregation"
	maggregation "github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/pipeline/applied"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/transformation"

	"github.com/mauricelam/genny/generic"
	"github.com/willf/bitset"
)

type typeSpecificAggregation interface {
	generic.Type

	// Add adds a new metric value.
	Add(value float64)

	// AddUnion adds a new metric value union.
	AddUnion(mu unaggregated.MetricUnion)

	// Count returns the number of values in the aggregation.
	Count() int64

	// ValueOf returns the value for the given aggregation type.
	ValueOf(aggType maggregation.Type) float64

	// Close closes the aggregation object.
	Close()
}

type genericElemPool interface {
	generic.Type

	// Put returns an element to a pool.
	Put(value *GenericElem)
}

type typeSpecificElemBase interface {
	generic.Type

	// Type returns the elem type.
	Type() metric.Type

	// FullPrefix returns the full prefix for the given metric type.
	FullPrefix(opts Options) []byte

	// DefaultAggregationTypes returns the default aggregation types.
	DefaultAggregationTypes(aggTypesOpts maggregation.TypesOptions) maggregation.Types

	// TypeStringFor returns the type string for the given aggregation type.
	TypeStringFor(aggTypesOpts maggregation.TypesOptions, aggType maggregation.Type) []byte

	// ElemPool returns the pool for the given element.
	ElemPool(opts Options) genericElemPool

	// NewAggregation creates a new aggregation.
	NewAggregation(opts Options, aggOpts raggregation.Options) typeSpecificAggregation

	// ResetSetData resets and sets data.
	ResetSetData(
		aggTypesOpts maggregation.TypesOptions,
		aggTypes maggregation.Types,
		useDefaultAggregation bool,
	) error

	// Close closes the element.
	Close()
}

type lockedAggregation struct {
	sync.Mutex

	closed       bool
	sourcesReady bool           // only used for elements receiving forwarded metrics
	sourcesSet   *bitset.BitSet // only used for elements receiving forwarded metrics
	consumeState consumeState   // only used for elements receiving forwarded metrics
	aggregation  typeSpecificAggregation
}

func (lockedAgg *lockedAggregation) close() {
	if lockedAgg.closed {
		return
	}
	lockedAgg.closed = true
	lockedAgg.sourcesSet = nil
	lockedAgg.aggregation.Close()
}

type timedAggregation struct {
	startAtNanos int64 // start time of an aggregation window
	lockedAgg    *lockedAggregation
}

func (ta *timedAggregation) Reset() {
	ta.startAtNanos = 0
	ta.lockedAgg = nil
}

// GenericElem is an element storing time-bucketed aggregations.
type GenericElem struct {
	elemBase
	typeSpecificElemBase

	values              []timedAggregation // metric aggregations sorted by time in ascending order
	toConsume           []timedAggregation // small buffer to avoid memory allocations during consumption
	lastConsumedAtNanos int64              // last consumed at in Unix nanoseconds
	lastConsumedValues  []float64          // last consumed values
}

// NewGenericElem creates a new element for the given metric type.
func NewGenericElem(
	IncomingMetricType IncomingMetricType,
	id id.RawID,
	sp policy.StoragePolicy,
	aggTypes maggregation.Types,
	pipeline applied.Pipeline,
	numForwardedTimes int,
	opts Options,
) (*GenericElem, error) {
	e := &GenericElem{
		elemBase: newElemBase(opts),
		values:   make([]timedAggregation, 0, defaultNumAggregations), // in most cases values will have two entries
	}
	if err := e.ResetSetData(IncomingMetricType, id, sp, aggTypes, pipeline, numForwardedTimes); err != nil {
		return nil, err
	}
	return e, nil
}

// MustNewGenericElem creates a new element, or panics if the input is invalid.
func MustNewGenericElem(
	IncomingMetricType IncomingMetricType,
	id id.RawID,
	sp policy.StoragePolicy,
	aggTypes maggregation.Types,
	pipeline applied.Pipeline,
	numForwardedTimes int,
	opts Options,
) *GenericElem {
	elem, err := NewGenericElem(IncomingMetricType, id, sp, aggTypes, pipeline, numForwardedTimes, opts)
	if err != nil {
		panic(fmt.Errorf("unable to create element: %v", err))
	}
	return elem
}

// ResetSetData resets the element and sets data.
func (e *GenericElem) ResetSetData(
	IncomingMetricType IncomingMetricType,
	id id.RawID,
	sp policy.StoragePolicy,
	aggTypes maggregation.Types,
	pipeline applied.Pipeline,
	numForwardedTimes int,
) error {
	useDefaultAggregation := aggTypes.IsDefault()
	if useDefaultAggregation {
		aggTypes = e.DefaultAggregationTypes(e.aggTypesOpts)
	}
	if err := e.elemBase.resetSetData(IncomingMetricType, id, sp, aggTypes, useDefaultAggregation, pipeline, numForwardedTimes); err != nil {
		return err
	}
	if err := e.typeSpecificElemBase.ResetSetData(e.aggTypesOpts, aggTypes, useDefaultAggregation); err != nil {
		return err
	}
	// If the pipeline contains derivative transformations, we need to store past
	// values in order to compute the derivatives.
	if !e.parsedPipeline.HasDerivativeTransform {
		return nil
	}
	numAggTypes := len(e.aggTypes)
	if cap(e.lastConsumedValues) < numAggTypes {
		e.lastConsumedValues = make([]float64, numAggTypes)
	}
	e.lastConsumedValues = e.lastConsumedValues[:numAggTypes]
	for i := 0; i < len(e.lastConsumedValues); i++ {
		e.lastConsumedValues[i] = nan
	}
	return nil
}

// AddUnion adds a metric value union at a given timestamp.
func (e *GenericElem) AddUnion(timestamp time.Time, mu unaggregated.MetricUnion) error {
	alignedStart := timestamp.Truncate(e.sp.Resolution().Window).UnixNano()
	lockedAgg, err := e.findOrCreate(alignedStart, sourcesOptions{})
	if err != nil {
		return err
	}
	lockedAgg.Lock()
	if lockedAgg.closed {
		lockedAgg.Unlock()
		return errAggregationClosed
	}
	lockedAgg.aggregation.AddUnion(mu)
	lockedAgg.Unlock()
	return nil
}

// AddUnique adds a metric value from a given source at a given timestamp.
// If previous values from the same source have already been added to the
// same aggregation, the incoming value is discarded.
// TODO(xichen): need warmpup time.
func (e *GenericElem) AddUnique(timestamp time.Time, values []float64, sourceID uint32) error {
	alignedStart := timestamp.Truncate(e.sp.Resolution().Window).UnixNano()
	lockedAgg, err := e.findOrCreate(alignedStart, sourcesOptions{updateSources: true, source: sourceID})
	if err != nil {
		return err
	}
	lockedAgg.Lock()
	if lockedAgg.closed {
		lockedAgg.Unlock()
		return errAggregationClosed
	}
	source := uint(sourceID)
	if !lockedAgg.sourcesReady {
		// If the sources are not ready, this is an empty source set to start with
		// so we need to set the source. If the corresponding bit is already set,
		// it implies a duplicate delivery from that source.
		if lockedAgg.sourcesSet.Test(source) {
			lockedAgg.Unlock()
			return errDuplicateForwardingSource
		}
		lockedAgg.sourcesSet.Set(source)
	} else {
		// Otherwise, this is a pre-filled source set to start with, so we need to
		// clear the source. If the corresponding bit is not set, it implies either
		// a duplicate delivery from that source, or a new source that was not in
		// the initial cloned source set.
		if !lockedAgg.sourcesSet.Test(source) {
			lockedAgg.Unlock()
			return errDuplicateOrNewForwardingSource
		}
		lockedAgg.sourcesSet.Clear(source)
		if lockedAgg.sourcesSet.None() {
			lockedAgg.consumeState = readyToConsume
		}
	}
	for _, v := range values {
		lockedAgg.aggregation.Add(v)
	}
	lockedAgg.Unlock()
	return nil
}

// Consume consumes values before a given time and removes them from the element
// after they are consumed, returning whether the element can be collected after
// the consumption is completed.
// NB: Consume is not thread-safe and must be called within a single goroutine
// to avoid race conditions.
func (e *GenericElem) Consume(
	targetNanos int64,
	isEarlierThanFn isEarlierThanFn,
	timestampNanosFn timestampNanosFn,
	flushLocalFn flushLocalMetricFn,
	flushForwardedFn flushForwardedMetricFn,
	onForwardedFlushedFn onForwardingElemFlushedFn,
) bool {
	resolution := e.sp.Resolution().Window
	e.Lock()
	if e.closed {
		e.Unlock()
		return false
	}
	idx := 0
	for range e.values {
		// Bail as soon as the timestamp is no later than the target time.
		timeNanos := timestampNanosFn(e.values[idx].startAtNanos, resolution)
		if !isEarlierThanFn(timeNanos, targetNanos) {
			break
		}
		idx++
	}
	e.toConsume = e.toConsume[:0]
	if idx > 0 {
		// Shift remaining values to the left and shrink the values slice.
		e.toConsume = append(e.toConsume, e.values[:idx]...)
		n := copy(e.values[0:], e.values[idx:])
		// Clear out the invalid items to avoid holding references to objects
		// for reduced GC overhead..
		for i := n; i < len(e.values); i++ {
			e.values[i].Reset()
		}
		e.values = e.values[:n]
	}
	canCollect := len(e.values) == 0 && e.tombstoned

	// Additionally for elements receiving forwarded metrics and sending aggregated metrics
	// to local backends, we also check if any aggregations are ready to be consumed. We however
	// do not remove the aggregations as we do for aggregations whose timestamps are old enough,
	// since for aggregations receiving forwarded metrics that are marked "consume ready", it is
	// possible that metrics still go to the such aggregation bucket after they are marked "consume
	// ready" due to delayed source re-delivery or new sources showing up, and removing such
	// aggregation prematurely would mean the values from the delayed sources and/or new sources
	// would be considered as the aggregated value for such aggregation bucket, which is incorrect.
	// By keeping such aggregation buckets and only removing them when they are considered old enough
	// (i.e., when their timestmaps are earlier than the target timestamp), we ensure no metrics may
	// go to such aggregation buckets after they are consumed and therefore avoid the aformentioned
	// problem.
	aggregationIdxToCloseUntil := len(e.toConsume)
	if e.IncomingMetricType == ForwardedIncomingMetric && e.isSourcesSetReadyWithLock() {
		e.maybeRefreshSourcesSetWithLock()
		// We only attempt to consume if the outgoing metrics type is local instead of forwarded.
		// This is because forwarded metrics are sent in batches and can only be sent when all sources
		// in the same shard have been consumed, and as such is not well suited for pre-emptive consumption.
		if e.outgoingMetricType() == localOutgoingMetric {
			for i := 0; i < len(e.values); i++ {
				// NB: This makes the logic easier to understand but it would be more efficient to use
				// an atomic here to avoid locking aggregations.
				e.values[i].lockedAgg.Lock()
				if e.values[i].lockedAgg.consumeState == readyToConsume {
					e.toConsume = append(e.toConsume, e.values[i])
					e.values[i].lockedAgg.consumeState = consuming
				}
				e.values[i].lockedAgg.Unlock()
			}
		}
	}
	e.Unlock()

	// Process the aggregations that are ready for consumption.
	for i := range e.toConsume {
		timeNanos := timestampNanosFn(e.toConsume[i].startAtNanos, resolution)
		e.toConsume[i].lockedAgg.Lock()
		if e.toConsume[i].lockedAgg.consumeState != consumed {
			e.processValueWithAggregationLock(timeNanos, e.toConsume[i].lockedAgg, flushLocalFn, flushForwardedFn)
		}
		e.toConsume[i].lockedAgg.consumeState = consumed
		if i < aggregationIdxToCloseUntil {
			e.toConsume[i].lockedAgg.close()
		}
		e.toConsume[i].lockedAgg.Unlock()
		e.toConsume[i].Reset()
	}

	if e.outgoingMetricType() == forwardedOutgoingMetric {
		forwardedAggregationKey, _ := e.ForwardedAggregationKey()
		onForwardedFlushedFn(e.onForwardedAggregationWrittenFn, forwardedAggregationKey)
	}

	return canCollect
}

// Close closes the element.
func (e *GenericElem) Close() {
	e.Lock()
	if e.closed {
		e.Unlock()
		return
	}
	e.closed = true
	e.id = nil
	e.parsedPipeline = parsedPipeline{}
	e.writeForwardedMetricFn = nil
	e.onForwardedAggregationWrittenFn = nil
	e.sourcesHeartbeat = nil
	e.sourcesSet = nil
	for idx := range e.values {
		// Close the underlying aggregation objects.
		e.values[idx].lockedAgg.close()
		e.values[idx].Reset()
	}
	e.values = e.values[:0]
	e.toConsume = e.toConsume[:0]
	e.lastConsumedValues = e.lastConsumedValues[:0]
	e.typeSpecificElemBase.Close()
	aggTypesPool := e.aggTypesOpts.TypesPool()
	pool := e.ElemPool(e.opts)
	e.Unlock()

	if !e.useDefaultAggregation {
		aggTypesPool.Put(e.aggTypes)
	}
	pool.Put(e)
}

// findOrCreate finds the aggregation for a given time, or creates one
// if it doesn't exist.
func (e *GenericElem) findOrCreate(
	alignedStart int64,
	sourcesOpts sourcesOptions,
) (*lockedAggregation, error) {
	e.RLock()
	if e.closed {
		e.RUnlock()
		return nil, errElemClosed
	}
	if sourcesOpts.updateSources {
		e.updateSources(sourcesOpts.source)
	}
	idx, found := e.indexOfWithLock(alignedStart)
	if found {
		agg := e.values[idx].lockedAgg
		e.RUnlock()
		return agg, nil
	}
	e.RUnlock()

	e.Lock()
	if e.closed {
		e.Unlock()
		return nil, errElemClosed
	}
	idx, found = e.indexOfWithLock(alignedStart)
	if found {
		agg := e.values[idx].lockedAgg
		e.Unlock()
		return agg, nil
	}

	// If not found, create a new aggregation.
	numValues := len(e.values)
	e.values = append(e.values, timedAggregation{})
	copy(e.values[idx+1:numValues+1], e.values[idx:numValues])

	var (
		sourcesReady = e.isSourcesSetReadyWithLock()
		sourcesSet   *bitset.BitSet
	)
	if sourcesOpts.updateSources {
		if sourcesReady {
			// If the sources set is ready, we clone it ane use the clone to
			// determine when we have received from all the expected sources.
			e.sourcesLock.Lock()
			sourcesSet = e.sourcesSet.Clone()
			e.sourcesLock.Unlock()
		} else {
			// Otherwise we create a new sources set and use it to filter out
			// duplicate delivery from the same sources.
			sourcesSet = bitset.New(defaultNumSources)
		}
	}
	e.values[idx] = timedAggregation{
		startAtNanos: alignedStart,
		lockedAgg: &lockedAggregation{
			sourcesReady: sourcesReady,
			sourcesSet:   sourcesSet,
			aggregation:  e.NewAggregation(e.opts, e.aggOpts),
		},
	}
	agg := e.values[idx].lockedAgg
	e.Unlock()
	return agg, nil
}

// indexOfWithLock finds the smallest element index whose timestamp
// is no smaller than the start time passed in, and true if it's an
// exact match, false otherwise.
func (e *GenericElem) indexOfWithLock(alignedStart int64) (int, bool) {
	numValues := len(e.values)
	// Optimize for the common case.
	if numValues > 0 && e.values[numValues-1].startAtNanos == alignedStart {
		return numValues - 1, true
	}
	// Binary search for the unusual case. We intentionally do not
	// use the sort.Search() function because it requires passing
	// in a closure.
	left, right := 0, numValues
	for left < right {
		mid := left + (right-left)/2 // avoid overflow
		if e.values[mid].startAtNanos < alignedStart {
			left = mid + 1
		} else {
			right = mid
		}
	}
	// If the current timestamp is equal to or larger than the target time,
	// return the index as is.
	if left < numValues && e.values[left].startAtNanos == alignedStart {
		return left, true
	}
	return left, false
}

func (e *GenericElem) processValueWithAggregationLock(
	timeNanos int64,
	lockedAgg *lockedAggregation,
	flushLocalFn flushLocalMetricFn,
	flushForwardedFn flushForwardedMetricFn,
) {
	if lockedAgg.aggregation.Count() == 0 {
		return
	}
	var (
		fullPrefix       = e.FullPrefix(e.opts)
		transformations  = e.parsedPipeline.Transformations
		discardNaNValues = e.opts.DiscardNaNAggregatedValues()
	)
	for aggTypeIdx, aggType := range e.aggTypes {
		value := lockedAgg.aggregation.ValueOf(aggType)
		for i := 0; i < transformations.Len(); i++ {
			transformType := transformations.At(i).Transformation.Type
			if transformType.IsUnaryTransform() {
				fn := transformType.MustUnaryTransform()
				res := fn(transformation.Datapoint{TimeNanos: timeNanos, Value: value})
				value = res.Value
			} else {
				fn := transformType.MustBinaryTransform()
				prev := transformation.Datapoint{TimeNanos: e.lastConsumedAtNanos, Value: e.lastConsumedValues[aggTypeIdx]}
				curr := transformation.Datapoint{TimeNanos: timeNanos, Value: value}
				res := fn(prev, curr)
				// NB: we only need to record the value needed for derivative transformations.
				// We currently only support first-order derivative transformations so we only
				// need to keep one value. In the future if we need to support higher-order
				// derivative transformations, we need to store an array of values here.
				e.lastConsumedValues[aggTypeIdx] = value
				value = res.Value
			}
		}
		if discardNaNValues && math.IsNaN(value) {
			continue
		}
		if e.outgoingMetricType() == localOutgoingMetric {
			flushLocalFn(fullPrefix, e.id, e.TypeStringFor(e.aggTypesOpts, aggType), timeNanos, value, e.sp)
		} else {
			forwardedAggregationKey, _ := e.ForwardedAggregationKey()
			flushForwardedFn(e.writeForwardedMetricFn, forwardedAggregationKey, timeNanos, value)
		}
	}
	e.lastConsumedAtNanos = timeNanos
}

func (e *GenericElem) outgoingMetricType() outgoingMetricType {
	if !e.parsedPipeline.HasRollup {
		return localOutgoingMetric
	}
	return forwardedOutgoingMetric
}

func (e *GenericElem) isSourcesSetReadyWithLock() bool {
	if e.buildingSourcesAtNanos == 0 {
		return false
	}
	// NB: Allow TTL for the source set to build up.
	return e.nowFn().UnixNano() >= e.buildingSourcesAtNanos+e.sourcesTTLNanos
}

func (e *GenericElem) maybeRefreshSourcesSetWithLock() {
	nowNanos := e.nowFn().UnixNano()
	if nowNanos-e.lastSourcesRefreshNanos < e.sourcesTTLNanos {
		return
	}
	e.sourcesLock.Lock()
	for sourceID, lastHeartbeatNanos := range e.sourcesHeartbeat {
		if nowNanos-lastHeartbeatNanos >= e.sourcesTTLNanos {
			delete(e.sourcesHeartbeat, sourceID)
			e.sourcesSet.Clear(uint(sourceID))
		}
	}
	e.lastSourcesRefreshNanos = nowNanos
	e.sourcesLock.Unlock()
}

func (e *GenericElem) updateSources(source uint32) {
	nowNanos := e.nowFn().UnixNano()
	e.sourcesLock.Lock()
	// First time a source is received.
	if e.sourcesHeartbeat == nil {
		e.sourcesHeartbeat = make(map[uint32]int64, defaultNumSources)
		e.sourcesSet = bitset.New(defaultNumSources)
		e.buildingSourcesAtNanos = nowNanos
		e.lastSourcesRefreshNanos = nowNanos
	}
	if v, exists := e.sourcesHeartbeat[source]; !exists || v < nowNanos {
		e.sourcesHeartbeat[source] = nowNanos
	}
	e.sourcesSet.Set(uint(source))
	e.sourcesLock.Unlock()
}
