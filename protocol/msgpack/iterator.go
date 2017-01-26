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
	"fmt"
	"io"

	"github.com/m3db/m3aggregator/metric"
	"github.com/m3db/m3aggregator/policy"
	"github.com/m3db/m3aggregator/pool"

	"gopkg.in/vmihailenco/msgpack.v2"
)

var (
	emptyResolution policy.Resolution
	emptyRetention  policy.Retention
)

// batchIterator is a batch iterator that uses MessagePack for decoding
// stream of batch-encoded metrics. It is NOT thread-safe. User must
// protect the iterator if accessed from multiple goroutines.
type batchIterator struct {
	decoder      *msgpack.Decoder         // internal decoder that does the actual decoding
	floatsPool   pool.FloatsPool          // pool for float slices
	policiesPool pool.PoliciesPool        // pool for policies
	typ          metric.Type              // metric type of the current metric
	counter      metric.Counter           // current counter
	timer        metric.Timer             // current timer
	batchTimer   metric.BatchTimer        // current batched timer
	gauge        metric.Gauge             // current gauge
	policies     policy.VersionedPolicies // current policies
	version      int                      // current version
	remaining    int                      // number of remaining items to decode in the current batch
	err          error                    // error encountered during decoding
}

// NewBatchIterator creates a batch iterator
func NewBatchIterator(reader io.Reader, opts IteratorOptions) (BatchIterator, error) {
	if opts == nil {
		opts = NewIteratorOptions()
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return &batchIterator{
		decoder:      msgpack.NewDecoder(reader),
		floatsPool:   opts.FloatsPool(),
		policiesPool: opts.PoliciesPool(),
	}, nil
}

func (it *batchIterator) Reset(reader io.Reader) {
	it.decoder.Reset(reader)
	it.remaining = 0
	it.err = nil
}

func (it *batchIterator) Version() int                                { return it.version }
func (it *batchIterator) Remaining() int                              { return it.remaining }
func (it *batchIterator) Type() metric.Type                           { return it.typ }
func (it *batchIterator) Counter() metric.Counter                     { return it.counter }
func (it *batchIterator) Timer() metric.Timer                         { return it.timer }
func (it *batchIterator) BatchTimer() metric.BatchTimer               { return it.batchTimer }
func (it *batchIterator) Gauge() metric.Gauge                         { return it.gauge }
func (it *batchIterator) VersionedPolicies() policy.VersionedPolicies { return it.policies }
func (it *batchIterator) Err() error                                  { return it.err }

func (it *batchIterator) Next() bool {
	if it.err != nil {
		return false
	}
	// If there are no more remaining items to decode in the current batch,
	// decode the next batch
	for it.err == nil && it.remaining <= 0 {
		it.decodeBatchStart()
	}
	it.decodeMetric()
	it.decodeVersionedPolicies()
	it.decodeEnd()
	return it.err == nil
}

func (it *batchIterator) decodeBatchStart() {
	it.decodeBatchSize()
	it.decodeVersion()
}

func (it *batchIterator) decodeMetric() {
	it.decodeType()
	if it.err != nil {
		return
	}
	switch it.typ {
	case metric.CounterType:
		it.decodeCounter()
	case metric.TimerType:
		it.decodeTimer()
	case metric.BatchTimerType:
		it.decodeBatchTimer()
	case metric.GaugeType:
		it.decodeGauge()
	default:
		it.err = fmt.Errorf("unrecognized metric type %v", it.typ)
	}
}

func (it *batchIterator) decodeCounter() {
	id := it.decodeID()
	value := int64(it.decodeVarint())
	it.counter = metric.Counter{ID: id, Value: value}
}

func (it *batchIterator) decodeTimer() {
	id := it.decodeID()
	value := it.decodeFloat64()
	it.timer = metric.Timer{ID: id, Value: value}
}

func (it *batchIterator) decodeBatchTimer() {
	id := it.decodeID()
	numValues := it.decodeArrayLen()
	if it.err != nil {
		return
	}
	values := it.floatsPool.Get(numValues)
	for i := 0; i < numValues; i++ {
		values = append(values, it.decodeFloat64())
	}
	it.batchTimer = metric.BatchTimer{ID: id, Values: values}
}

func (it *batchIterator) decodeGauge() {
	id := it.decodeID()
	value := it.decodeFloat64()
	it.gauge = metric.Gauge{ID: id, Value: value}
}

func (it *batchIterator) decodeVersionedPolicies() {
	version := int(it.decodeVarint())
	if it.err != nil {
		return
	}
	// NB(xichen): if the policy version is the default version, simply
	// return the default policies
	if version == policy.DefaultPolicyVersion {
		it.policies = policy.DefaultVersionedPolicies
		return
	}
	numPolicies := it.decodeArrayLen()
	if it.err != nil {
		return
	}
	policies := it.policiesPool.Get(numPolicies)
	for i := 0; i < numPolicies; i++ {
		policies = append(policies, it.decodePolicy())
	}
	it.policies = policy.VersionedPolicies{Version: version, Policies: policies}
}

func (it *batchIterator) decodeEnd() {
	if it.err != nil {
		return
	}
	it.remaining--
}

func (it *batchIterator) decodeVersion() {
	it.version = int(it.decodeVarint())
	if it.err != nil {
		return
	}
	if it.version > supportedVersion {
		it.err = fmt.Errorf("decoded version %d is higher than supported version %d", it.version, supportedVersion)
	}
}

func (it *batchIterator) decodeBatchSize() {
	it.remaining = int(it.decodeVarint())
}

func (it *batchIterator) decodeType() {
	it.typ = metric.Type(it.decodeVarint())
}

func (it *batchIterator) decodeID() metric.IDType {
	return metric.IDType(it.decodeBytes())
}

func (it *batchIterator) decodePolicy() policy.Policy {
	resolution := it.decodeResolution()
	retention := it.decodeRetention()
	return policy.Policy{Resolution: resolution, Retention: retention}
}

func (it *batchIterator) decodeResolution() policy.Resolution {
	resolutionValue := policy.ResolutionValue(it.decodeVarint())
	resolution, err := resolutionValue.Resolution()
	if it.err != nil {
		return emptyResolution
	}
	it.err = err
	return resolution
}

func (it *batchIterator) decodeRetention() policy.Retention {
	retentionValue := policy.RetentionValue(it.decodeVarint())
	retention, err := retentionValue.Retention()
	if it.err != nil {
		return emptyRetention
	}
	it.err = err
	return retention
}

// NB(xichen): the underlying msgpack decoder implementation
// always decodes an int64 and looks at the actual decoded
// value to determine the width of the integer (a.k.a. varint
// decoding)
func (it *batchIterator) decodeVarint() int64 {
	if it.err != nil {
		return 0
	}
	value, err := it.decoder.DecodeInt64()
	it.err = err
	return value
}

func (it *batchIterator) decodeFloat64() float64 {
	if it.err != nil {
		return 0.0
	}
	value, err := it.decoder.DecodeFloat64()
	it.err = err
	return value
}

func (it *batchIterator) decodeBytes() []byte {
	if it.err != nil {
		return nil
	}
	value, err := it.decoder.DecodeBytes()
	it.err = err
	return value
}

func (it *batchIterator) decodeArrayLen() int {
	if it.err != nil {
		return 0
	}
	value, err := it.decoder.DecodeArrayLen()
	it.err = err
	return value
}
