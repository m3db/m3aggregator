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

package aggregation

import "github.com/m3db/m3metrics/policy"

var (
	defaultOptions = Options{IsDefault: true, IsExpensive: false}
)

// Options is the options for aggregations.
type Options struct {
	// IsDefault means only default aggregation types are enabled.
	IsDefault bool
	// IsExpensive means expensive (multiplication／division)
	// aggregation types are enabled.
	IsExpensive bool
}

// NewOptions creates a new aggregation options.
func NewOptions() Options {
	return defaultOptions
}

// ResetSetData resets the aggregation options.
func (o *Options) ResetSetData(aggTypes policy.AggregationTypes) {
	o.IsDefault = aggTypes.IsDefault()
	o.IsExpensive = isExpensive(aggTypes)
}

func isExpensive(aggTypes policy.AggregationTypes) bool {
	for _, aggType := range aggTypes {
		if aggType == policy.SumSq || aggType == policy.Stdev {
			return true
		}
	}
	return false
}
