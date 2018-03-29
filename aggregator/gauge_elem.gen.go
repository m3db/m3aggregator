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

// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package aggregator

import (
	"time"

	maggregation "github.com/m3db/m3metrics/aggregation"

	"github.com/m3db/m3metrics/metric/id"

	"github.com/m3db/m3metrics/metric/unaggregated"

	"github.com/m3db/m3metrics/op/applied"

	"github.com/m3db/m3metrics/policy"
)

type timedGauge struct {
	timeNanos   int64
	aggregation *lockedGauge
}

func (ta *timedGauge) Reset() {
	ta.timeNanos = 0
	ta.aggregation = nil
}

// GaugeElem is an element storing time-bucketed aggregations.
type GaugeElem struct {
	elemBase
	gaugeElemBase

	values    []timedGauge // metric aggregations sorted by time in ascending order
	toConsume []timedGauge
}

// NewGaugeElem creates a new element for the given metric type.
func NewGaugeElem(
	id id.RawID,
	sp policy.StoragePolicy,
	aggTypes maggregation.Types,
	pipeline applied.Pipeline,
	opts Options,
) *GaugeElem {
	e := &GaugeElem{
		elemBase: newElemBase(opts),
		values:   make([]timedGauge, 0, defaultNumValues), // in most cases values will have two entries
	}
	e.ResetSetData(id, sp, aggTypes, pipeline)
	return e
}

// ResetSetData resets the element and sets data.
// TODO(xichen): handle pipelines properly.
func (e *GaugeElem) ResetSetData(
	id id.RawID,
	sp policy.StoragePolicy,
	aggTypes maggregation.Types,
	pipeline applied.Pipeline,
) {
	useDefaultAggregation := aggTypes.IsDefault()
	if useDefaultAggregation {
		aggTypes = e.DefaultAggregationTypes(e.aggTypesOpts)
	}

	e.gaugeElemBase.ResetSetData(e.aggTypesOpts, aggTypes, useDefaultAggregation)
	e.elemBase.resetSetData(id, sp, aggTypes, useDefaultAggregation)
}

// AddMetric adds a new value.
func (e *GaugeElem) AddMetric(timestamp time.Time, mu unaggregated.MetricUnion) error {
	alignedStart := timestamp.Truncate(e.sp.Resolution().Window).UnixNano()
	agg, err := e.findOrCreate(alignedStart)
	if err != nil {
		return err
	}
	agg.Lock()
	agg.Add(mu)
	agg.Unlock()
	return nil
}

// Consume processes values before a given time and discards
// them afterwards, returning whether the element can be collected
// after consuming the values.
func (e *GaugeElem) Consume(earlierThanNanos int64, fn aggMetricFn) bool {
	e.Lock()
	if e.closed {
		e.Unlock()
		return false
	}
	idx := 0
	for range e.values {
		// Bail as soon as the timestamp is no later than the target time.
		if e.values[idx].timeNanos >= earlierThanNanos {
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
	e.Unlock()

	// Process the aggregations that are ready for consumption.
	for i := range e.toConsume {
		endAtNanos := e.toConsume[i].timeNanos + int64(e.sp.Resolution().Window)
		e.processValue(endAtNanos, e.toConsume[i].aggregation, fn)
		// Closes the aggregation object after it's processed.
		e.toConsume[i].aggregation.Close()
		e.toConsume[i].Reset()
	}

	return canCollect
}

// Close closes the element.
func (e *GaugeElem) Close() {
	e.Lock()
	if e.closed {
		e.Unlock()
		return
	}
	e.closed = true
	e.id = nil
	for idx := range e.values {
		// Close the underlying aggregation objects.
		e.values[idx].aggregation.Close()
		e.values[idx].Reset()
	}
	e.values = e.values[:0]
	e.toConsume = e.toConsume[:0]
	e.gaugeElemBase.Close()
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
func (e *GaugeElem) findOrCreate(alignedStart int64) (*lockedGauge, error) {
	e.RLock()
	if e.closed {
		e.RUnlock()
		return nil, errElemClosed
	}
	idx, found := e.indexOfWithLock(alignedStart)
	if found {
		agg := e.values[idx].aggregation
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
		agg := e.values[idx].aggregation
		e.Unlock()
		return agg, nil
	}

	// If not found, create a new aggregation.
	numValues := len(e.values)
	e.values = append(e.values, timedGauge{})
	copy(e.values[idx+1:numValues+1], e.values[idx:numValues])
	e.values[idx] = timedGauge{
		timeNanos:   alignedStart,
		aggregation: e.NewLockedAggregation(e.opts, e.aggOpts),
	}
	agg := e.values[idx].aggregation
	e.Unlock()
	return agg, nil
}

// indexOfWithLock finds the smallest element index whose timestamp
// is no smaller than the start time passed in, and true if it's an
// exact match, false otherwise.
func (e *GaugeElem) indexOfWithLock(alignedStart int64) (int, bool) {
	numValues := len(e.values)
	// Optimize for the common case.
	if numValues > 0 && e.values[numValues-1].timeNanos == alignedStart {
		return numValues - 1, true
	}
	// Binary search for the unusual case. We intentionally do not
	// use the sort.Search() function because it requires passing
	// in a closure.
	left, right := 0, numValues
	for left < right {
		mid := left + (right-left)/2 // avoid overflow
		if e.values[mid].timeNanos < alignedStart {
			left = mid + 1
		} else {
			right = mid
		}
	}
	// If the current timestamp is equal to or larger than the target time,
	// return the index as is.
	if left < numValues && e.values[left].timeNanos == alignedStart {
		return left, true
	}
	return left, false
}

func (e *GaugeElem) processValue(timeNanos int64, agg *lockedGauge, fn aggMetricFn) {
	var fullPrefix = e.FullPrefix(e.opts)
	if e.useDefaultAggregation {
		// NB(cw) Use default suffix slice for faster look up.
		suffixes := e.DefaultAggregationTypeStrings(e.aggTypesOpts)
		aggTypes := e.DefaultAggregationTypes(e.aggTypesOpts)
		agg.Lock()
		for i, aggType := range aggTypes {
			fn(fullPrefix, e.id, suffixes[i], timeNanos, agg.ValueOf(aggType), e.sp)
		}
		agg.Unlock()
		return
	}

	agg.Lock()
	for _, aggType := range e.aggTypes {
		fn(fullPrefix, e.id, e.TypeStringFor(e.aggTypesOpts, aggType), timeNanos, agg.ValueOf(aggType), e.sp)
	}
	agg.Unlock()
}
