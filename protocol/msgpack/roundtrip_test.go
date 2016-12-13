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
	"testing"
	"time"

	"github.com/m3db/m3aggregator/metric"
	"github.com/m3db/m3aggregator/policy"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

type testEncodeFn func(t *testing.T, encoder BatchEncoder)
type testDecodeFn func(t *testing.T, iterator BatchIterator)
type onTestDoneFn func(t *testing.T, iterator BatchIterator)

type metricWithPolicies struct {
	metric   interface{}
	policies policy.VersionedPolicies
}

type batchedInput struct {
	batchSize int
	inputs    []metricWithPolicies
}

func testBatchEncoder(t *testing.T) BatchEncoder {
	opts := NewEncoderOptions()
	encoder, err := NewBatchEncoder(opts)
	require.NoError(t, err)
	return encoder
}

func testBatchIterator(t *testing.T, reader io.Reader) BatchIterator {
	opts := NewIteratorOptions()
	iterator, err := NewBatchIterator(reader, opts)
	require.NoError(t, err)
	return iterator
}

func validateRoundtrip(t *testing.T, inputs ...metricWithPolicies) {
	type inputFn func(encoder BatchEncoder)
	type resultFn func(iterator BatchIterator)

	var (
		count         int
		version       int
		remaining     int
		inputFns      []inputFn
		resultFns     []resultFn
		expectedTypes []metric.Type
		resultTypes   []metric.Type
		results       []metricWithPolicies
	)

	for _, input := range inputs {
		input := input
		switch typedInput := input.metric.(type) {
		case metric.Counter:
			expectedTypes = append(expectedTypes, metric.CounterType)
			inputFns = append(inputFns, func(encoder BatchEncoder) {
				encoder.EncodeCounter(typedInput, input.policies)
			})
			resultFns = append(resultFns, func(iterator BatchIterator) {
				result := iterator.Counter()
				policies := iterator.VersionedPolicies()
				results = append(results, metricWithPolicies{metric: result, policies: policies})
			})
		case metric.Timer:
			expectedTypes = append(expectedTypes, metric.TimerType)
			inputFns = append(inputFns, func(encoder BatchEncoder) {
				encoder.EncodeTimer(typedInput, input.policies)
			})
			resultFns = append(resultFns, func(iterator BatchIterator) {
				result := iterator.Timer()
				policies := iterator.VersionedPolicies()
				results = append(results, metricWithPolicies{metric: result, policies: policies})
			})
		case metric.BatchTimer:
			expectedTypes = append(expectedTypes, metric.BatchTimerType)
			inputFns = append(inputFns, func(encoder BatchEncoder) {
				encoder.EncodeBatchTimer(typedInput, input.policies)
			})
			resultFns = append(resultFns, func(iterator BatchIterator) {
				result := iterator.BatchTimer()
				policies := iterator.VersionedPolicies()
				results = append(results, metricWithPolicies{metric: result, policies: policies})
			})
		case metric.Gauge:
			expectedTypes = append(expectedTypes, metric.GaugeType)
			inputFns = append(inputFns, func(encoder BatchEncoder) {
				encoder.EncodeGauge(typedInput, input.policies)
			})
			resultFns = append(resultFns, func(iterator BatchIterator) {
				result := iterator.Gauge()
				policies := iterator.VersionedPolicies()
				results = append(results, metricWithPolicies{metric: result, policies: policies})
			})
		default:
			require.Fail(t, "unrecognized metric type")
		}
	}

	// Encode the batch of metrics
	encoder := testBatchEncoder(t)
	require.NoError(t, encoder.StartBatch(len(inputs)))
	for _, fn := range inputFns {
		fn(encoder)
	}
	batch := encoder.EndBatch()

	// Decode the batch of metrics
	byteStream := bytes.NewBuffer(batch.Buffer.Bytes())
	iterator := testBatchIterator(t, byteStream)
	for iterator.Next() {
		version = iterator.Version()
		remaining = iterator.Remaining()
		resultTypes = append(resultTypes, iterator.Type())
		resultFns[count](iterator)
		count++
	}

	// Assert the results match expectations
	require.Equal(t, io.EOF, iterator.Err())
	require.Equal(t, len(inputs), count)
	require.Equal(t, supportedVersion, version)
	require.Equal(t, 0, remaining)
	require.Equal(t, expectedTypes, resultTypes)
	require.Equal(t, inputs, results)
}

func TestEncodeDecodeCounterWithDefaultPolicies(t *testing.T) {
	validateRoundtrip(t, metricWithPolicies{
		metric: metric.Counter{
			ID:    []byte("foo"),
			Value: 1234,
		},
		policies: policy.DefaultVersionedPolicies,
	})
}

func TestEncodeDecodeTimerWithDefaultPolicies(t *testing.T) {
	validateRoundtrip(t, metricWithPolicies{
		metric: metric.Timer{
			ID:    []byte("foo"),
			Value: 345.67,
		},
		policies: policy.DefaultVersionedPolicies,
	})
}

func TestEncodeDecodeBatchTimerWithDefaultPolicies(t *testing.T) {
	validateRoundtrip(t, metricWithPolicies{
		metric: metric.BatchTimer{
			ID:     []byte("foo"),
			Values: []float64{222.22, 345.67, 901.23345},
		},
		policies: policy.DefaultVersionedPolicies,
	})
}

func TestEncodeDecodeGaugeWithDefaultPolicies(t *testing.T) {
	validateRoundtrip(t, metricWithPolicies{
		metric: metric.Gauge{
			ID:    []byte("foo"),
			Value: 123.456,
		},
		policies: policy.DefaultVersionedPolicies,
	})
}

func TestEncodeDecodeAllTypesWithDefaultPolicies(t *testing.T) {
	inputs := []metricWithPolicies{
		{
			metric: metric.Counter{
				ID:    []byte("foo"),
				Value: 1234,
			},
			policies: policy.DefaultVersionedPolicies,
		},
		{
			metric: metric.Timer{
				ID:    []byte("foo"),
				Value: 345.67,
			},
			policies: policy.DefaultVersionedPolicies,
		},
		{
			metric: metric.BatchTimer{
				ID:     []byte("foo"),
				Values: []float64{222.22, 345.67, 901.23345},
			},
			policies: policy.DefaultVersionedPolicies,
		},
		{
			metric: metric.Gauge{
				ID:    []byte("foo"),
				Value: 123.456,
			},
			policies: policy.DefaultVersionedPolicies,
		},
	}
	validateRoundtrip(t, inputs...)
}

func TestEncodeDecodeAllTypesWithCustomPolicies(t *testing.T) {
	inputs := []metricWithPolicies{
		// Retain this metric at 1 second resolution for 1 hour
		{
			metric: metric.Counter{
				ID:    []byte("foo"),
				Value: 1234,
			},
			policies: policy.VersionedPolicies{
				Version: 1,
				Policies: []policy.Policy{
					{
						Resolution: policy.Resolution{Window: time.Duration(1), Precision: xtime.Second},
						Retention:  policy.Retention(time.Hour),
					},
				},
			},
		},
		// Retain this metric at 10 second resolution for 6 hours,
		// then 1 minute resolution for 2 days
		{
			metric: metric.Timer{
				ID:    []byte("foo"),
				Value: 345.67,
			},
			policies: policy.VersionedPolicies{
				Version: 2,
				Policies: []policy.Policy{
					{
						Resolution: policy.Resolution{Window: time.Duration(10), Precision: xtime.Second},
						Retention:  policy.Retention(6 * time.Hour),
					},
					{
						Resolution: policy.Resolution{Window: time.Duration(1), Precision: xtime.Minute},
						Retention:  policy.Retention(2 * 24 * time.Hour),
					},
				},
			},
		},
		// Retain this metric using the default policies
		{
			metric: metric.BatchTimer{
				ID:     []byte("foo"),
				Values: []float64{222.22, 345.67, 901.23345},
			},
			policies: policy.DefaultVersionedPolicies,
		},
		// Retain this metric at 10 minute resolution for 45 days
		{
			metric: metric.Gauge{
				ID:    []byte("foo"),
				Value: 123.456,
			},
			policies: policy.VersionedPolicies{
				Version: 2,
				Policies: []policy.Policy{
					{
						Resolution: policy.Resolution{Window: time.Duration(10), Precision: xtime.Minute},
						Retention:  policy.Retention(45 * 24 * time.Hour),
					},
				},
			},
		},
	}
	validateRoundtrip(t, inputs...)
}
