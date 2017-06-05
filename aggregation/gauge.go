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

package aggregation

import "sync"

var (
	emptyGauge Gauge
)

// Gauge aggregates gauge values.
type Gauge struct {
	value float64 // latest value received
}

// NewGauge creates a new gauge.
func NewGauge() Gauge { return emptyGauge }

// Set sets the gauge value.
func (g *Gauge) Set(value float64) { g.value = value }

// Value returns the latest value.
func (g *Gauge) Value() float64 { return g.value }

// LockedGauge is a locked gauge.
type LockedGauge struct {
	sync.Mutex
	Gauge
}

// NewLockedGauge creates a new locked gauge.
func NewLockedGauge() *LockedGauge { return &LockedGauge{} }

// Reset resets the locked gauge.
func (lg *LockedGauge) Reset() { lg.Gauge = emptyGauge }
