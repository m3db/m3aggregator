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

package sharding

import (
	"sync"
	"testing"

	"github.com/m3db/m3metrics/metric/id"

	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/require"
)

func TestMurmur32HashAggregatedShardFn(t *testing.T) {
	hashType := Murmur32Hash
	numShards := 1024
	aggregatedShardFn, err := hashType.AggregatedShardFn()
	require.NoError(t, err)

	// Verify the aggregated shard function is thread-safe and the computed
	// shards match expectation.
	var wg sync.WaitGroup
	numWorkers := 100
	inputs := []id.ChunkedID{
		{Prefix: []byte(""), Data: []byte("bar"), Suffix: []byte("")},
		{Prefix: []byte("foo"), Data: []byte("bar"), Suffix: []byte("")},
		{Prefix: []byte(""), Data: []byte("bar"), Suffix: []byte("baz")},
		{Prefix: []byte("foo"), Data: []byte("bar"), Suffix: []byte("baz")},
	}
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for _, input := range inputs {
				d := murmur3.New32()
				d.Write(input.Prefix)
				d.Write(input.Data)
				d.Write(input.Suffix)
				expected := d.Sum32() % uint32(numShards)
				actual := aggregatedShardFn(input, numShards)
				require.Equal(t, expected, actual)
			}
		}()
	}
	wg.Wait()
}
