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

package aggregator

import (
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/shard"
)

type openFn func() error
type placementFn func() (placement.Placement, error)
type instanceFn func() (placement.Instance, error)
type instanceFromFn func(placement placement.Placement) (placement.Instance, error)
type hasReplacementInstanceFn func() (bool, error)
type shardsFn func() (shard.Shards, error)

type mockPlacementManager struct {
	openFn                   openFn
	placementFn              placementFn
	instanceFn               instanceFn
	instanceFromFn           instanceFromFn
	hasReplacementInstanceFn hasReplacementInstanceFn
	shardsFn                 shardsFn
}

func (m *mockPlacementManager) Open() error { return m.openFn() }

func (m *mockPlacementManager) Placement() (placement.Placement, error) { return m.placementFn() }

func (m *mockPlacementManager) Instance() (placement.Instance, error) { return m.instanceFn() }

func (m *mockPlacementManager) InstanceFrom(
	placement placement.Placement,
) (placement.Instance, error) {
	return m.instanceFromFn(placement)
}

func (m *mockPlacementManager) HasReplacementInstance() (bool, error) {
	return m.hasReplacementInstanceFn()
}

func (m *mockPlacementManager) Shards() (shard.Shards, error) { return m.shardsFn() }

func (m *mockPlacementManager) Close() error { return nil }
