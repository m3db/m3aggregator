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

package config

import (
	"errors"
	"fmt"
	"time"

	"github.com/m3db/m3aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3aggregator/services/m3aggregator/handler"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
)

var (
	errUnknownQuantileSuffixFnType = errors.New("unknown quantile suffix function type")
	errUnknownFlushFnType          = errors.New("unknown flush function type")
	errNoForwardFlushConfiguration = errors.New("no forward flush configuration")
	emptyPolicy                    policy.Policy
)

// AggregatorConfiguration contains aggregator configuration.
type AggregatorConfiguration struct {
	// Common metric prefix.
	MetricPrefix string `yaml:"metricPrefix"`

	// Counter metric prefix.
	CounterPrefix string `yaml:"counterPrefix"`

	// Timer metric prefix.
	TimerPrefix string `yaml:"timerPrefix"`

	// Timer sum metric suffix.
	TimerSumSuffix string `yaml:"timerSumSuffix"`

	// Timer sum square metric suffix.
	TimerSumSqSuffix string `yaml:"timerSumSqSuffix"`

	// Timer sum mean metric suffix.
	TimerMeanSuffix string `yaml:"timerMeanSuffix"`

	// Timer lower metric suffix.
	TimerLowerSuffix string `yaml:"timerLowerSuffix"`

	// Timer upper metric suffix.
	TimerUpperSuffix string `yaml:"timerUpperSuffix"`

	// Timer count metric suffix.
	TimerCountSuffix string `yaml:"timerCountSuffix"`

	// Timer standard deviation metric suffix.
	TimerStdevSuffix string `yaml:"timerStdevSuffix"`

	// Timer median metric suffix.
	TimerMedianSuffix string `yaml:"timerMedianSuffix"`

	// Timer quantile suffix function type.
	TimerQuantileSuffixFnType string `yaml:"timerQuantileSuffixFnType"`

	// Gauge metric suffix.
	GaugePrefix string `yaml:"gaugePrefix"`

	// Stream configuration for computing quantiles.
	Stream streamConfiguration `yaml:"stream"`

	// Minimum flush interval across all resolutions.
	MinFlushInterval time.Duration `yaml:"minFlushInterval"`

	// Maximum flush size in bytes.
	MaxFlushSize int `yaml:"maxFlushSize"`

	// Flushing configuration.
	Flush flushConfiguration `yaml:"flush"`

	// EntryTTL determines how long an entry remains alive before it may be expired due to inactivity.
	EntryTTL time.Duration `yaml:"entryTTL"`

	// EntryCheckInterval determines how often entries are checked for expiration.
	EntryCheckInterval time.Duration `yaml:"entryCheckInterval"`

	// EntryCheckBatchPercent determines the percentage of entries checked in a batch.
	EntryCheckBatchPercent float64 `yaml:"entryCheckBatchPercent" validate:"min=0.0,max=1.0"`

	// Pool of entries.
	EntryPool pool.ObjectPoolConfiguration `yaml:"entryPool"`

	// Pool of counter elements.
	CounterElemPool pool.ObjectPoolConfiguration `yaml:"counterElemPool"`

	// Pool of timer elements.
	TimerElemPool pool.ObjectPoolConfiguration `yaml:"timerElemPool"`

	// Pool of gauge elements.
	GaugeElemPool pool.ObjectPoolConfiguration `yaml:"gaugeElemPool"`

	// Pool of buffered encoders.
	BufferedEncoderPool pool.ObjectPoolConfiguration `yaml:"bufferedEncoderPool"`
}

// NewAggregatorOptions creates a new set of aggregator options.
func (c *AggregatorConfiguration) NewAggregatorOptions(
	instrumentOpts instrument.Options,
) (aggregator.Options, error) {
	opts := aggregator.NewOptions().SetInstrumentOptions(instrumentOpts)

	// Set the prefix and suffix for metrics aggregations.
	opts = setMetricPrefixOrSuffix(opts, c.MetricPrefix, opts.SetMetricPrefix)
	opts = setMetricPrefixOrSuffix(opts, c.CounterPrefix, opts.SetCounterPrefix)
	opts = setMetricPrefixOrSuffix(opts, c.TimerPrefix, opts.SetTimerPrefix)
	opts = setMetricPrefixOrSuffix(opts, c.TimerSumSuffix, opts.SetTimerSumSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.TimerSumSqSuffix, opts.SetTimerSumSqSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.TimerMeanSuffix, opts.SetTimerMeanSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.TimerLowerSuffix, opts.SetTimerLowerSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.TimerUpperSuffix, opts.SetTimerUpperSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.TimerCountSuffix, opts.SetTimerCountSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.TimerStdevSuffix, opts.SetTimerStdevSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.TimerMedianSuffix, opts.SetTimerMedianSuffix)
	opts = setMetricPrefixOrSuffix(opts, c.GaugePrefix, opts.SetGaugePrefix)

	// Set timer quantile suffix function.
	quantileSuffixFn, err := c.parseTimerQuantileSuffixFn()
	if err != nil {
		return nil, err
	}
	opts = opts.SetTimerQuantileSuffixFn(quantileSuffixFn)

	// Set stream options.
	iOpts := instrumentOpts.SetMetricsScope(instrumentOpts.MetricsScope().SubScope("stream"))
	streamOpts, err := c.Stream.NewStreamOptions(iOpts)
	if err != nil {
		return nil, err
	}
	opts = opts.SetStreamOptions(streamOpts)

	// Set flushing options.
	if c.MinFlushInterval != 0 {
		opts = opts.SetMinFlushInterval(c.MinFlushInterval)
	}
	if c.MaxFlushSize != 0 {
		opts = opts.SetMaxFlushSize(c.MaxFlushSize)
	}
	flushFn, err := c.Flush.NewFlushFn()
	if err != nil {
		return nil, err
	}
	opts = opts.SetFlushFn(flushFn)

	// Set entry options.
	if c.EntryTTL != 0 {
		opts = opts.SetEntryTTL(c.EntryTTL)
	}
	if c.EntryCheckInterval != 0 {
		opts = opts.SetEntryCheckInterval(c.EntryCheckInterval)
	}
	if c.EntryCheckBatchPercent != 0.0 {
		opts = opts.SetEntryCheckBatchPercent(c.EntryCheckBatchPercent)
	}

	// Set entry pool.
	iOpts = instrumentOpts.SetMetricsScope(instrumentOpts.MetricsScope().SubScope("entry-pool"))
	entryPoolOpts := c.EntryPool.NewObjectPoolOptions(iOpts)
	entryPool := aggregator.NewEntryPool(entryPoolOpts)
	opts = opts.SetEntryPool(entryPool)
	entryPool.Init(func() *aggregator.Entry { return aggregator.NewEntry(nil, opts) })

	// Set counter elem pool.
	iOpts = instrumentOpts.SetMetricsScope(instrumentOpts.MetricsScope().SubScope("counter-elem-pool"))
	counterElemPoolOpts := c.CounterElemPool.NewObjectPoolOptions(iOpts)
	counterElemPool := aggregator.NewCounterElemPool(counterElemPoolOpts)
	opts = opts.SetCounterElemPool(counterElemPool)
	counterElemPool.Init(func() *aggregator.CounterElem { return aggregator.NewCounterElem(nil, emptyPolicy, opts) })

	// Set timer elem pool.
	iOpts = instrumentOpts.SetMetricsScope(instrumentOpts.MetricsScope().SubScope("timer-elem-pool"))
	timerElemPoolOpts := c.TimerElemPool.NewObjectPoolOptions(iOpts)
	timerElemPool := aggregator.NewTimerElemPool(timerElemPoolOpts)
	opts = opts.SetTimerElemPool(timerElemPool)
	timerElemPool.Init(func() *aggregator.TimerElem { return aggregator.NewTimerElem(nil, emptyPolicy, opts) })

	// Set gauge eleme pool.
	iOpts = instrumentOpts.SetMetricsScope(instrumentOpts.MetricsScope().SubScope("gauge-elem-pool"))
	gaugeElemPoolOpts := c.GaugeElemPool.NewObjectPoolOptions(iOpts)
	gaugeElemPool := aggregator.NewGaugeElemPool(gaugeElemPoolOpts)
	opts = opts.SetGaugeElemPool(gaugeElemPool)
	gaugeElemPool.Init(func() *aggregator.GaugeElem { return aggregator.NewGaugeElem(nil, emptyPolicy, opts) })

	// Set buffered encoder pool.
	iOpts = instrumentOpts.SetMetricsScope(instrumentOpts.MetricsScope().SubScope("buffered-encoder-pool"))
	bufferedEncoderPoolOpts := c.BufferedEncoderPool.NewObjectPoolOptions(iOpts)
	bufferedEncoderPool := msgpack.NewBufferedEncoderPool(bufferedEncoderPoolOpts)
	opts = opts.SetBufferedEncoderPool(bufferedEncoderPool)
	bufferedEncoderPool.Init(func() msgpack.BufferedEncoder { return msgpack.NewPooledBufferedEncoder(bufferedEncoderPool) })

	return opts, nil
}

// parseTimerQuantileSuffixFn parses the quantile suffix function type.
func (c *AggregatorConfiguration) parseTimerQuantileSuffixFn() (aggregator.QuantileSuffixFn, error) {
	fnType := defaultQuantileSuffixType
	if c.TimerQuantileSuffixFnType != "" {
		fnType = timerQuantileSuffixFnType(c.TimerQuantileSuffixFnType)
	}
	switch fnType {
	case defaultQuantileSuffixType:
		return defaultTimerQuantileSuffixFn, nil
	default:
		return nil, errUnknownQuantileSuffixFnType
	}
}

// streamConfiguration contains configuration for quantile-related metric streams.
type streamConfiguration struct {
	// Error epsilon for quantile computation.
	Eps float64 `yaml:"eps"`

	// Target quantiles to compute.
	Quantiles []float64 `yaml:"quantiles"`

	// Initial heap capacity for quantile computation.
	Capacity int `yaml:"capacity"`

	// Pool of streams.
	StreamPool pool.ObjectPoolConfiguration `yaml:"streamPool"`

	// Pool of metric samples.
	SamplePool pool.ObjectPoolConfiguration `yaml:"samplePool"`

	// Pool of float slices.
	FloatsPool pool.BucketizedPoolConfiguration `yaml:"floatsPool"`
}

func (c *streamConfiguration) NewStreamOptions(instrumentOpts instrument.Options) (cm.Options, error) {
	opts := cm.NewOptions().
		SetEps(c.Eps).
		SetQuantiles(c.Quantiles).
		SetCapacity(c.Capacity)

	iOpts := instrumentOpts.SetMetricsScope(instrumentOpts.MetricsScope().SubScope("sample-pool"))
	samplePoolOpts := c.SamplePool.NewObjectPoolOptions(iOpts)
	samplePool := cm.NewSamplePool(samplePoolOpts)
	opts = opts.SetSamplePool(samplePool)
	samplePool.Init()

	iOpts = instrumentOpts.SetMetricsScope(instrumentOpts.MetricsScope().SubScope("floats-pool"))
	floatsPoolOpts := c.FloatsPool.NewObjectPoolOptions(iOpts)
	floatsPool := pool.NewFloatsPool(c.FloatsPool.NewBuckets(), floatsPoolOpts)
	opts = opts.SetFloatsPool(floatsPool)
	floatsPool.Init()

	iOpts = instrumentOpts.SetMetricsScope(instrumentOpts.MetricsScope().SubScope("stream-pool"))
	streamPoolOpts := c.StreamPool.NewObjectPoolOptions(iOpts)
	streamPool := cm.NewStreamPool(streamPoolOpts)
	opts = opts.SetStreamPool(streamPool)
	streamPool.Init(func() cm.Stream { return cm.NewStream(opts) })

	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return opts, nil
}

// flushConfiguration contains configuration for flushing metrics.
type flushConfiguration struct {
	// Flushing function type.
	Type string `yaml:"type" validate:"regexp=(^blackhole$|^logging$|^forward$)"`

	// Forward flush configuration.
	Forward *forwardFlushConfiguration `yaml:"forward"`
}

func (c *flushConfiguration) NewFlushFn() (aggregator.FlushFn, error) {
	switch flushFnType(c.Type) {
	case blackholeFlushFnType:
		return handler.NewBlackholeHandler().Handle, nil
	case loggingFlushFnType:
		return handler.NewLoggingHandler().Handle, nil
	case forwardFlushFnType:
		if c.Forward == nil {
			return nil, errNoForwardFlushConfiguration
		}
		return c.Forward.NewFlushFn(), nil
	default:
		return nil, errUnknownFlushFnType
	}
}

// forwardFlushConfiguration contains configuration for forward flushing function.
type forwardFlushConfiguration struct {
	// Server address list.
	Servers []string `yaml:"servers"`
}

// TODO(xichen): implement this.
func (c *forwardFlushConfiguration) NewFlushFn() aggregator.FlushFn {
	return nil
}

// timerQuantileSuffixFnType is the timer quantile suffix function type.
type timerQuantileSuffixFnType string

// A list of supported timer quantile suffix function types.
const (
	defaultQuantileSuffixType timerQuantileSuffixFnType = "default"
)

func defaultTimerQuantileSuffixFn(quantile float64) []byte {
	return []byte(fmt.Sprintf(".p%0.0f", quantile*100))
}

// flushFnType is the flushing function type.
type flushFnType string

// A list of supported flushing function types.
const (
	blackholeFlushFnType flushFnType = "blackhole"
	loggingFlushFnType   flushFnType = "logging"
	forwardFlushFnType   flushFnType = "forward"
)

type metricPrefixOrSuffixSetter func(prefixOrSuffix []byte) aggregator.Options

func setMetricPrefixOrSuffix(
	opts aggregator.Options,
	str string,
	fn metricPrefixOrSuffixSetter,
) aggregator.Options {
	if str == "" {
		return opts
	}
	return fn([]byte(str))
}
