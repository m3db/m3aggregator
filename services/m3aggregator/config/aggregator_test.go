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
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3metrics/metric/id"

	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestHashFnTypePartitionFnGen(t *testing.T) {
	hashFnType := murmur32HashFn
	totalPartitions := 1024
	partitionFnGen, err := hashFnType.PartitionFnGen(totalPartitions)
	require.NoError(t, err)

	// Generate n partition functions.
	numWorkers := 100
	partitionFns := make([]aggregator.PartitionFn, numWorkers)
	for i := 0; i < numWorkers; i++ {
		partitionFns[i] = partitionFnGen()
	}

	// Verify the generated partition function is thread-safe and the computed
	// partitions match expectation.
	var wg sync.WaitGroup
	inputs := []id.ChunkedID{
		{Prefix: []byte(""), Data: []byte("bar"), Suffix: []byte("")},
		{Prefix: []byte("foo"), Data: []byte("bar"), Suffix: []byte("")},
		{Prefix: []byte(""), Data: []byte("bar"), Suffix: []byte("baz")},
		{Prefix: []byte("foo"), Data: []byte("bar"), Suffix: []byte("baz")},
	}
	for i := 0; i < numWorkers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			for _, input := range inputs {
				d := murmur3.New32()
				d.Write(input.Prefix)
				d.Write(input.Data)
				d.Write(input.Suffix)
				expected := d.Sum32() % uint32(totalPartitions)
				actual := partitionFns[i](input)
				require.Equal(t, expected, actual)
			}
		}()
	}
	wg.Wait()
}

func TestJitterBuckets(t *testing.T) {
	config := `
    - flushInterval: 1m
      maxJitterPercent: 1.0
    - flushInterval: 10m
      maxJitterPercent: 0.5
    - flushInterval: 1h
      maxJitterPercent: 0.25`

	var buckets jitterBuckets
	require.NoError(t, yaml.Unmarshal([]byte(config), &buckets))

	maxJitterFn, err := buckets.NewMaxJitterFn()
	require.NoError(t, err)

	inputs := []struct {
		interval          time.Duration
		expectedMaxJitter time.Duration
	}{
		{interval: time.Second, expectedMaxJitter: time.Second},
		{interval: 10 * time.Second, expectedMaxJitter: 10 * time.Second},
		{interval: time.Minute, expectedMaxJitter: time.Minute},
		{interval: 10 * time.Minute, expectedMaxJitter: 5 * time.Minute},
		{interval: time.Hour, expectedMaxJitter: 15 * time.Minute},
		{interval: 6 * time.Hour, expectedMaxJitter: 90 * time.Minute},
	}
	for _, input := range inputs {
		require.Equal(t, input.expectedMaxJitter, maxJitterFn(input.interval))
	}
}
