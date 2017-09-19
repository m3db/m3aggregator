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

package partitioning

import (
	"testing"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestParsePartitionSetErrors(t *testing.T) {
	tests := []struct {
		yaml        string
		expectedErr string
	}{
		{yaml: `huh`, expectedErr: "invalid range 'huh'"},
		{yaml: `2..1`, expectedErr: "invalid range: 2 > 1"},
	}

	for _, test := range tests {
		var partitions PartitionSet
		err := yaml.Unmarshal([]byte(test.yaml), &partitions)
		require.Error(t, err)
		require.Equal(t, test.expectedErr, err.Error())
		require.Equal(t, 0, len(partitions))
	}
}

func TestParsePartitionSet(t *testing.T) {
	tests := []struct {
		yaml     string
		expected []uint32
	}{
		{yaml: `partitions: 76`, expected: []uint32{76}},
		{yaml: `partitions: [3, 6, 5]`, expected: []uint32{3, 5, 6}},
		{yaml: `partitions: ["3"]`, expected: []uint32{3}},
		{yaml: `partitions: ["3..8"]`, expected: []uint32{3, 4, 5, 6, 7, 8}},
		{yaml: `partitions: ["3", "3..8"]`, expected: []uint32{3, 4, 5, 6, 7, 8}},
		{yaml: `partitions: ["3", "3..8", 9]`, expected: []uint32{3, 4, 5, 6, 7, 8, 9}},
		{yaml: `partitions: 3`, expected: []uint32{3}},
		{yaml: `partitions: "3"`, expected: []uint32{3}},
		{yaml: `partitions: "3..8"`, expected: []uint32{3, 4, 5, 6, 7, 8}},
	}

	for i, test := range tests {
		var cfg struct {
			Partitions PartitionSet
		}

		err := yaml.Unmarshal([]byte(test.yaml), &cfg)
		require.NoError(t, err, "received error for test %d", i)

		expectedSet := make(PartitionSet)
		for _, p := range test.expected {
			expectedSet.Add(p)
		}

		require.Equal(t, expectedSet, cfg.Partitions, "invalid results for test %d", i)
		for _, partition := range test.expected {
			require.True(t, cfg.Partitions.Contains(partition), "%v does not contain %d", cfg.Partitions, partition)
		}
	}
}

func TestPartitionSetMinMax(t *testing.T) {
	tests := []struct {
		yaml        string
		expectedMin int
		expectedMax int
	}{
		{yaml: "", expectedMin: -1, expectedMax: -1},
		{yaml: `1..1`, expectedMin: 1, expectedMax: 1},
		{yaml: `20..30`, expectedMin: 20, expectedMax: 30},
		{yaml: `[20, 5, 6, 30]`, expectedMin: 5, expectedMax: 30},
	}
	for _, test := range tests {
		var partitions PartitionSet
		err := yaml.Unmarshal([]byte(test.yaml), &partitions)
		require.NoError(t, err)
		require.Equal(t, test.expectedMin, partitions.Min())
		require.Equal(t, test.expectedMax, partitions.Max())
	}
}
