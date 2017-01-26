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

package msgpack

import (
	"bytes"
	"io"

	"github.com/m3db/m3aggregator/metric"
	"github.com/m3db/m3aggregator/policy"
	"github.com/m3db/m3aggregator/pool"

	"gopkg.in/vmihailenco/msgpack.v2"
)

const (
	supportedVersion int = 1
)

// Encoder is a msgpack-based encoder
type Encoder interface {
	// EncodeCounter encodes a counter with applicable policies
	EncodeCounter(c metric.Counter, p policy.VersionedPolicies) error

	// EncodeTimer encodes a timer with applicable policies
	EncodeTimer(t metric.Timer, p policy.VersionedPolicies) error

	// EncodeBatchTimer encodes a batched timer with applicable policies
	EncodeBatchTimer(t metric.BatchTimer, p policy.VersionedPolicies) error

	// EncodeGauge encodes a gauge with applicable policies
	EncodeGauge(g metric.Gauge, p policy.VersionedPolicies) error
}

// BatchEncoder encodes metrics in batches
type BatchEncoder interface {
	Encoder

	// StartBatch starts the batch encoding process
	StartBatch(batchSize int) error

	// EndBatch ends the batch encoding process, returns the
	// encoded batch of metrics stored in the byte buffer, and
	// resets the internal buffer
	EndBatch() BufferedEncoder
}

// BufferedEncoder is an messagePack-based encoder backed by byte buffers
type BufferedEncoder struct {
	*msgpack.Encoder

	Buffer *bytes.Buffer
}

// BufferedEncoderAlloc allocates a bufferer encoder
type BufferedEncoderAlloc func() BufferedEncoder

// BufferedEncoderPool is a pool of buffered encoders
type BufferedEncoderPool interface {
	// Init initializes the buffered encoder pool
	Init(alloc BufferedEncoderAlloc)

	// Get returns a buffered encoder from the pool
	Get() BufferedEncoder

	// Put puts a buffered encoder into the pool
	Put(enc BufferedEncoder)
}

// EncoderOptions provide options for encoders
type EncoderOptions interface {
	// SetBufferedEncoderPool sets the buffered encoder pool
	SetBufferedEncoderPool(value BufferedEncoderPool) EncoderOptions

	// BufferedEncoderPool returns the buffered encoder pool
	BufferedEncoderPool() BufferedEncoderPool

	// Validate validates the options
	Validate() error
}

// BatchIterator iterates over data stream and decodes
// metrics encoded in batches
type BatchIterator interface {
	// Next returns true if there are more items to decode
	Next() bool

	// Version returns the current version
	Version() int

	// Remaining returns the number of remaining items in the current batch
	Remaining() int

	// Type returns the current metric type
	Type() metric.Type

	// Counter returns the counter value
	Counter() metric.Counter

	// Timer returns the current timer value
	Timer() metric.Timer

	// BatchTimer returns the current batched timer value
	BatchTimer() metric.BatchTimer

	// Gauge returns the current gauge value
	Gauge() metric.Gauge

	// VersionedPolicies returns the current versioned policies
	VersionedPolicies() policy.VersionedPolicies

	// Err returns the error encountered during decoding if any
	Err() error

	// Reset resets the iterator
	Reset(reader io.Reader)
}

// IteratorOptions provide options for iterators
type IteratorOptions interface {
	// SetFloatsPool sets the floats pool
	SetFloatsPool(value pool.FloatsPool) IteratorOptions

	// FloatsPool returns the floats pool
	FloatsPool() pool.FloatsPool

	// SetPoliciesPool sets the policies pool
	SetPoliciesPool(value pool.PoliciesPool) IteratorOptions

	// PoliciesPool returns the policies pool
	PoliciesPool() pool.PoliciesPool

	// Validate validates the options
	Validate() error
}
