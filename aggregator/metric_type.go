// Copyright (c) 2018 Uber Technologies, Inc.
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

package aggregator

type incomingMetricType int

const (
	// standardIncomingMetric is a standard (currently untimed) incoming metric.
	standardIncomingMetric incomingMetricType = iota

	// forwardedIncomingMetric is a forwarded incoming metric.
	forwardedIncomingMetric
)

func (t incomingMetricType) String() string {
	switch t {
	case standardIncomingMetric:
		return "standardIncomingMetric"
	case forwardedIncomingMetric:
		return "forwardedIncomingMetric"
	default:
		// Should never get here.
		return "unknown"
	}
}

type outgoingMetricType int

const (
	// localOutgoingMetric is an outgoing metric that gets flushed to backends
	// locally known to the server.
	localOutgoingMetric outgoingMetricType = iota

	// forwardedOutgoingMetric is an outgoing metric that gets forwarded to
	// other aggregation servers for further aggregation and rollup.
	forwardedOutgoingMetric
)

func (t outgoingMetricType) String() string {
	switch t {
	case localOutgoingMetric:
		return "localOutgoingMetric"
	case forwardedOutgoingMetric:
		return "forwardedOutgoingMetric"
	default:
		// Should never get here.
		return "unknown"
	}
}
