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

package writer

import (
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
)

const (
	defaultMaxBufferSize            = 1440
	defaultIncludeEncodingTime      = false
	defaultEncodingTimeSamplingRate = 0.01
)

// Options provide a set of options for the writer.
type Options interface {
	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) Options

	// ClockOptions returns the clock options.
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetMaxBufferSize sets the maximum buffer size.
	SetMaxBufferSize(value int) Options

	// MaxBufferSize returns the maximum buffer size.
	MaxBufferSize() int

	// SetBufferedEncoderPool sets the buffered encoder pool.
	SetBufferedEncoderPool(value msgpack.BufferedEncoderPool) Options

	// BufferedEncoderPool returns the buffered encoder pool.
	BufferedEncoderPool() msgpack.BufferedEncoderPool

	// SetIncludeEncodingTime sets whether the time at which metrics and policies are
	// encoded is included alongside the encoded data. Such encoding time can be used
	// to compute end-to-end latencies for example.
	SetIncludeEncodingTime(value bool) Options

	// IncludeEncodingTime returns whether the time at which metrics and policies are
	// encoded is included alongside the encoded data. Such encoding time can be used
	// to compute end-to-end latencies for example.
	IncludeEncodingTime() bool

	// SetEncodingTimeSampleRate sets the sampling rate at which the encoding time is
	// included in the encoded data. This option only applies when including encoding
	// time is enabled.
	SetEncodingTimeSamplingRate(value float64) Options

	// EncodingTimeSamplingRate returns the sampling rate at which the encoding time is
	// included in the encoded data. This option only applies when including encoding
	// time is enabled.
	EncodingTimeSamplingRate() float64
}

type options struct {
	clockOpts                clock.Options
	instrumentOpts           instrument.Options
	maxBufferSize            int
	bufferedEncoderPool      msgpack.BufferedEncoderPool
	includeEncodingTime      bool
	encodingTimeSamplingRate float64
}

// NewOptions provide a set of writer options.
func NewOptions() Options {
	bufferedEncoderPool := msgpack.NewBufferedEncoderPool(nil)
	bufferedEncoderPool.Init(func() msgpack.BufferedEncoder {
		return msgpack.NewPooledBufferedEncoder(bufferedEncoderPool)
	})
	return &options{
		clockOpts:                clock.NewOptions(),
		instrumentOpts:           instrument.NewOptions(),
		maxBufferSize:            defaultMaxBufferSize,
		bufferedEncoderPool:      bufferedEncoderPool,
		includeEncodingTime:      defaultIncludeEncodingTime,
		encodingTimeSamplingRate: defaultEncodingTimeSamplingRate,
	}
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

func (o *options) SetMaxBufferSize(value int) Options {
	opts := *o
	opts.maxBufferSize = value
	return &opts
}

func (o *options) MaxBufferSize() int {
	return o.maxBufferSize
}

func (o *options) SetBufferedEncoderPool(value msgpack.BufferedEncoderPool) Options {
	opts := *o
	opts.bufferedEncoderPool = value
	return &opts
}

func (o *options) BufferedEncoderPool() msgpack.BufferedEncoderPool {
	return o.bufferedEncoderPool
}

func (o *options) SetIncludeEncodingTime(value bool) Options {
	opts := *o
	opts.includeEncodingTime = value
	return &opts
}

func (o *options) IncludeEncodingTime() bool {
	return o.includeEncodingTime
}

func (o *options) SetEncodingTimeSamplingRate(value float64) Options {
	opts := *o
	opts.encodingTimeSamplingRate = value
	return &opts
}

func (o *options) EncodingTimeSamplingRate() float64 {
	return o.encodingTimeSamplingRate
}
