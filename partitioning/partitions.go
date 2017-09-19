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
	"errors"
	"fmt"
	"regexp"
	"strconv"
)

const (
	defaultNumPartitions = 32
)

var (
	// Partition range is expected to provided in the form of startPartition..endPartition.
	// An example partition range is 0..63.
	rangeRegexp = regexp.MustCompile(`^([0-9]+)(\.\.([0-9]+))?$`)

	errInvalidPartition = errors.New("invalid partition")
)

// A PartitionSet is a range of partitions organized as a set.
type PartitionSet map[uint32]struct{}

// UnmarshalYAML unmarshals YAML into a partition set.
func (ps *PartitionSet) UnmarshalYAML(f func(interface{}) error) error {
	*ps = make(PartitionSet, defaultNumPartitions)

	// If YAML contains a single string, attempt to parse out a single range.
	var s string
	if err := f(&s); err == nil {
		return ps.ParseRange(s)
	}

	// Otherwise try to parse out a list of ranges.
	var a []interface{}
	if err := f(&a); err == nil {
		for _, v := range a {
			switch c := v.(type) {
			case string:
				if err := ps.ParseRange(c); err != nil {
					return err
				}
			case int:
				ps.Add(uint32(c))
			default:
				return fmt.Errorf("unexpected range %v", c)
			}
		}
		return nil
	}

	// Otherwise try to parse out a single partition.
	var n int
	if err := f(&n); err == nil {
		ps.Add(uint32(n))
		return nil
	}

	return errInvalidPartition
}

// Min returns the minimum partition contained by the partition set,
// or -1 if the partition set is empty.
func (ps PartitionSet) Min() int {
	minPartition := -1
	for p := range ps {
		if minPartition == -1 || minPartition > int(p) {
			minPartition = int(p)
		}
	}
	return minPartition
}

// Max returns the maximum partition contained by the partition set,
// or -1 if the partition set is empty.
func (ps PartitionSet) Max() int {
	maxPartition := -1
	for p := range ps {
		if maxPartition < int(p) {
			maxPartition = int(p)
		}
	}
	return maxPartition
}

// Contains returns true if the partition set contains the given partition.
func (ps PartitionSet) Contains(p uint32) bool {
	_, found := ps[p]
	return found
}

// Add adds the partition to the set.
func (ps PartitionSet) Add(p uint32) {
	ps[p] = struct{}{}
}

// AddBetween adds partitions between the given min (inclusive) and max (exclusive).
func (ps PartitionSet) AddBetween(minInclusive, maxExclusive uint32) {
	for i := minInclusive; i < maxExclusive; i++ {
		ps.Add(i)
	}
}

// ParseRange parses a range of partitions and adds them to the set.
func (ps PartitionSet) ParseRange(s string) error {
	rangeMatches := rangeRegexp.FindStringSubmatch(s)
	if len(rangeMatches) != 0 {
		return ps.addRange(rangeMatches)
	}

	return fmt.Errorf("invalid range '%s'", s)
}

func (ps PartitionSet) addRange(matches []string) error {
	min, err := strconv.ParseInt(matches[1], 10, 32)
	if err != nil {
		return err
	}

	max := min
	if matches[3] != "" {
		max, err = strconv.ParseInt(matches[3], 10, 32)
		if err != nil {
			return err
		}
	}

	if min > max {
		return fmt.Errorf("invalid range: %d > %d", min, max)
	}

	ps.AddBetween(uint32(min), uint32(max)+1)
	return nil
}
