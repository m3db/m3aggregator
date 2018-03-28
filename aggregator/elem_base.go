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
	"errors"
	"sync"
	"time"

	raggregation "github.com/m3db/m3aggregator/aggregation"
	maggregation "github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/pool"
)

const (
	// Default number of aggregation buckets allocated initially.
	defaultNumValues = 2
)

var (
	errElemClosed = errors.New("element is closed")
)

type aggMetricFn func(
	idPrefix []byte,
	id id.RawID,
	idSuffix []byte,
	timeNanos int64,
	value float64,
	sp policy.StoragePolicy,
)

// metricElem is the common interface for metric elements.
type metricElem interface {
	// ID returns the metric id.
	ID() id.RawID

	// ResetSetData resets the element and sets data.
	ResetSetData(id id.RawID, sp policy.StoragePolicy, aggTypes maggregation.Types)

	// AddMetric adds a new metric value.
	// TODO(xichen): a value union would suffice here.
	AddMetric(timestamp time.Time, mu unaggregated.MetricUnion) error

	// Consume processes values before a given time and discards
	// them afterwards, returning whether the element can be collected
	// after discarding the values.
	Consume(earlierThanNanos int64, fn aggMetricFn) bool

	// MarkAsTombstoned marks an element as tombstoned, which means this element
	// will be deleted once its aggregated values have been flushed.
	MarkAsTombstoned()

	// Close closes the element.
	Close()
}

type elemBase struct {
	sync.RWMutex

	opts                  Options
	aggTypesOpts          maggregation.TypesOptions
	id                    id.RawID
	sp                    policy.StoragePolicy
	aggTypes              maggregation.Types
	aggOpts               raggregation.Options
	useDefaultAggregation bool
	tombstoned            bool
	closed                bool
}

func newElemBase(opts Options) elemBase {
	return elemBase{
		opts:         opts,
		aggTypesOpts: opts.AggregationTypesOptions(),
		aggOpts:      raggregation.NewOptions(),
	}
}

// resetSetData resets the element base and sets data.
func (e *elemBase) resetSetData(id id.RawID, sp policy.StoragePolicy, aggTypes maggregation.Types, useDefaultAggregation bool) {
	e.id = id
	e.sp = sp
	e.aggTypes = aggTypes
	e.useDefaultAggregation = useDefaultAggregation
	e.aggOpts.ResetSetData(aggTypes)
	e.tombstoned = false
	e.closed = false
}

// ID returns the metric id.
func (e *elemBase) ID() id.RawID {
	e.RLock()
	id := e.id
	e.RUnlock()
	return id
}

// MarkAsTombstoned marks an element as tombstoned, which means this element
// will be deleted once its aggregated values have been flushed.
func (e *elemBase) MarkAsTombstoned() {
	e.Lock()
	if e.closed {
		e.Unlock()
		return
	}
	e.tombstoned = true
	e.Unlock()
}

type counterElemBase struct{}

func (e counterElemBase) FullPrefix(opts Options) []byte { return opts.FullCounterPrefix() }

func (e counterElemBase) DefaultAggregationTypeStrings(aggTypesOpts maggregation.TypesOptions) [][]byte {
	return aggTypesOpts.DefaultCounterAggregationTypeStrings()
}

func (e counterElemBase) DefaultAggregationTypes(aggTypesOpts maggregation.TypesOptions) maggregation.Types {
	return aggTypesOpts.DefaultCounterAggregationTypes()
}

func (e counterElemBase) TypeStringFor(aggTypesOpts maggregation.TypesOptions, aggType maggregation.Type) []byte {
	return aggTypesOpts.TypeStringForCounter(aggType)
}

func (e counterElemBase) ElemPool(opts Options) CounterElemPool { return opts.CounterElemPool() }

func (e counterElemBase) NewLockedAggregation(_ Options, aggOpts raggregation.Options) *lockedCounter {
	return newLockedCounter(raggregation.NewCounter(aggOpts))
}

func (e *counterElemBase) ResetSetData(maggregation.TypesOptions, maggregation.Types, bool) {}
func (e *counterElemBase) Close()                                                           {}

type timerElemBase struct {
	isQuantilesPooled bool
	quantiles         []float64
	quantilesPool     pool.FloatsPool
}

func (e timerElemBase) FullPrefix(opts Options) []byte { return opts.FullTimerPrefix() }

func (e timerElemBase) DefaultAggregationTypeStrings(aggTypesOpts maggregation.TypesOptions) [][]byte {
	return aggTypesOpts.DefaultTimerAggregationTypeStrings()
}

func (e timerElemBase) DefaultAggregationTypes(aggTypesOpts maggregation.TypesOptions) maggregation.Types {
	return aggTypesOpts.DefaultTimerAggregationTypes()
}

func (e timerElemBase) TypeStringFor(aggTypesOpts maggregation.TypesOptions, aggType maggregation.Type) []byte {
	return aggTypesOpts.TypeStringForTimer(aggType)
}

func (e timerElemBase) ElemPool(opts Options) TimerElemPool { return opts.TimerElemPool() }

func (e timerElemBase) NewLockedAggregation(opts Options, aggOpts raggregation.Options) *lockedTimer {
	newTimer := raggregation.NewTimer(e.quantiles, opts.StreamOptions(), aggOpts)
	return newLockedTimer(newTimer)
}

func (e *timerElemBase) ResetSetData(
	aggTypesOpts maggregation.TypesOptions,
	aggTypes maggregation.Types,
	useDefaultAggregation bool,
) {
	if useDefaultAggregation {
		e.quantiles = aggTypesOpts.TimerQuantiles()
		e.isQuantilesPooled = false
		return
	}

	e.quantilesPool = aggTypesOpts.QuantilesPool()
	e.quantiles, e.isQuantilesPooled = aggTypes.PooledQuantiles(e.quantilesPool)
}

func (e *timerElemBase) Close() {
	if e.isQuantilesPooled {
		e.quantilesPool.Put(e.quantiles)
	}
	e.quantiles = nil
	e.quantilesPool = nil
}

type gaugeElemBase struct{}

func (e gaugeElemBase) FullPrefix(opts Options) []byte { return opts.FullGaugePrefix() }

func (e gaugeElemBase) DefaultAggregationTypeStrings(aggTypesOpts maggregation.TypesOptions) [][]byte {
	return aggTypesOpts.DefaultGaugeAggregationTypeStrings()
}

func (e gaugeElemBase) DefaultAggregationTypes(aggTypesOpts maggregation.TypesOptions) maggregation.Types {
	return aggTypesOpts.DefaultGaugeAggregationTypes()
}

func (e gaugeElemBase) TypeStringFor(aggTypesOpts maggregation.TypesOptions, aggType maggregation.Type) []byte {
	return aggTypesOpts.TypeStringForGauge(aggType)
}

func (e gaugeElemBase) ElemPool(opts Options) GaugeElemPool { return opts.GaugeElemPool() }

func (e gaugeElemBase) NewLockedAggregation(_ Options, aggOpts raggregation.Options) *lockedGauge {
	return newLockedGauge(raggregation.NewGauge(aggOpts))
}

func (e *gaugeElemBase) ResetSetData(maggregation.TypesOptions, maggregation.Types, bool) {}
func (e *gaugeElemBase) Close()                                                           {}
