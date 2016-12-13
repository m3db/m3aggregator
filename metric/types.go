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

package metric

// Type is a metric type
type Type int8

// List of supported metric types
const (
	UnknownType Type = iota
	CounterType
	TimerType
	BatchTimerType
	GaugeType
)

// IDType is the metric id type
type IDType []byte

// String is the string representation of an id
func (id IDType) String() string { return string(id) }

// Counter is a counter containing the counter ID and the counter value
type Counter struct {
	ID    IDType
	Value int64
}

// Timer is a timer containing the timer ID and a timer value
type Timer struct {
	ID    IDType
	Value float64
}

// BatchTimer is a timer containing the timer ID and a list of timer values
type BatchTimer struct {
	ID     IDType
	Values []float64
}

// Gauge is a gauge containing the gauge ID and the value at certain time
type Gauge struct {
	ID    IDType
	Value float64
}
