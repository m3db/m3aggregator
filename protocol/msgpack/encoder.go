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
	"fmt"

	"github.com/m3db/m3aggregator/metric"
	"github.com/m3db/m3aggregator/policy"

	"gopkg.in/vmihailenco/msgpack.v2"
)

// newBufferedEncoder creates a new buffered encoder
func newBufferedEncoder() BufferedEncoder {
	buffer := bytes.NewBuffer(nil)

	return BufferedEncoder{
		Buffer:  buffer,
		Encoder: msgpack.NewEncoder(buffer),
	}
}

// batchEncoder is a batch encoder that uses MessagePack for encoding.
// It is NOT thread-safe. User must protect the encoder if accessed from
// multiple goroutines.
type batchEncoder struct {
	opts        EncoderOptions      // encoding options
	encoderPool BufferedEncoderPool // pool for internal encoders
	encoder     BufferedEncoder     // internal encoder that does the actual encoding
	err         error               // error encountered during encoding
}

// NewBatchEncoder creates a new batch encoder
func NewBatchEncoder(opts EncoderOptions) (BatchEncoder, error) {
	if opts == nil {
		opts = NewEncoderOptions()
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	encoderPool := opts.BufferedEncoderPool()
	return &batchEncoder{
		opts:        opts,
		encoderPool: encoderPool,
		encoder:     encoderPool.Get(),
	}, nil
}

// StartBatch starts the encoding process
func (enc *batchEncoder) StartBatch(batchSize int) error {
	// If the batch size is not positive, this is an invalid request
	// so we return an error but don't set error in the encoder since
	// this doesn't corrupt the encoded byte stream
	if batchSize <= 0 {
		return fmt.Errorf("batch size must be positive but is instead %d", batchSize)
	}
	enc.encodeBatchSize(batchSize)
	enc.encodeVersion(supportedVersion)
	return enc.err
}

func (enc *batchEncoder) EncodeCounter(c metric.Counter, p policy.VersionedPolicies) error {
	enc.encodeType(metric.CounterType)
	enc.encodeID(c.ID)
	enc.encodeVarint(c.Value)
	enc.encodeVersionedPolicies(p)
	return enc.err
}

func (enc *batchEncoder) EncodeTimer(t metric.Timer, p policy.VersionedPolicies) error {
	enc.encodeType(metric.TimerType)
	enc.encodeID(t.ID)
	enc.encodeFloat64(t.Value)
	enc.encodeVersionedPolicies(p)
	return enc.err
}

func (enc *batchEncoder) EncodeBatchTimer(bt metric.BatchTimer, p policy.VersionedPolicies) error {
	enc.encodeType(metric.BatchTimerType)
	enc.encodeID(bt.ID)
	enc.encodeArrayLen(len(bt.Values))
	for _, v := range bt.Values {
		enc.encodeFloat64(v)
	}
	enc.encodeVersionedPolicies(p)
	return enc.err
}

func (enc *batchEncoder) EncodeGauge(g metric.Gauge, p policy.VersionedPolicies) error {
	enc.encodeType(metric.GaugeType)
	enc.encodeID(g.ID)
	enc.encodeFloat64(g.Value)
	enc.encodeVersionedPolicies(p)
	return enc.err
}

func (enc *batchEncoder) encodeVersionedPolicies(p policy.VersionedPolicies) {
	enc.encodeVersion(p.Version)
	// NB(xichen): if this is a default policy, we only encode the policy version
	// and not the actual policies to optimize for the common case where the policies
	// are the default ones
	if p.Version == policy.DefaultPolicyVersion {
		return
	}
	enc.encodeArrayLen(len(p.Policies))
	for _, policy := range p.Policies {
		enc.encodePolicy(policy)
	}
}

func (enc *batchEncoder) EndBatch() BufferedEncoder {
	encoder := enc.encoder
	enc.encoder = enc.encoderPool.Get()
	return encoder
}

func (enc *batchEncoder) encodeVersion(version int) {
	enc.encodeVarint(int64(version))
}

func (enc *batchEncoder) encodeBatchSize(batchSize int) {
	enc.encodeVarint(int64(batchSize))
}

func (enc *batchEncoder) encodeType(typ metric.Type) {
	enc.encodeVarint(int64(typ))
}

func (enc *batchEncoder) encodeID(id metric.IDType) {
	enc.encoder.EncodeBytes([]byte(id))
}

func (enc *batchEncoder) encodePolicy(p policy.Policy) {
	enc.encodeResolution(p.Resolution)
	enc.encodeRetention(p.Retention)
}

func (enc *batchEncoder) encodeResolution(resolution policy.Resolution) {
	if enc.err != nil {
		return
	}
	resolutionValue, err := policy.ValueFromResolution(resolution)
	if err != nil {
		enc.err = err
		return
	}
	enc.encodeVarint(int64(resolutionValue))
}

func (enc *batchEncoder) encodeRetention(retention policy.Retention) {
	if enc.err != nil {
		return
	}
	retentionValue, err := policy.ValueFromRetention(retention)
	if err != nil {
		enc.err = err
		return
	}
	enc.encodeVarint(int64(retentionValue))
}

// NB(xichen): the underlying msgpack encoder implementation
// always cast an integer value to an int64 and encodes integer
// values as varints, regardless of the actual integer type
func (enc *batchEncoder) encodeVarint(value int64) {
	if enc.err != nil {
		return
	}
	enc.err = enc.encoder.EncodeInt64(value)
}

func (enc *batchEncoder) encodeFloat64(value float64) {
	if enc.err != nil {
		return
	}
	enc.err = enc.encoder.EncodeFloat64(value)
}

func (enc *batchEncoder) encodeBytes(value []byte) {
	if enc.err != nil {
		return
	}
	enc.err = enc.encoder.EncodeBytes(value)
}

func (enc *batchEncoder) encodeArrayLen(value int) {
	if enc.err != nil {
		return
	}
	enc.err = enc.encoder.EncodeArrayLen(value)
}
