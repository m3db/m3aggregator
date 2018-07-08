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

package common

import (
	"testing"
	"time"

	"github.com/m3db/m3cluster/generated/proto/commonpb"
	"github.com/m3db/m3cluster/kv/mem"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"
)

func TestTrafficControllerWithoutInitialKVValue(t *testing.T) {
	defer leaktest.Check(t)()

	store := mem.NewStore()
	key := "testKey"
	opts := NewTrafficControlOptions().
		SetStore(store).
		SetRuntimeKey(key).
		SetInitTimeout(200 * time.Millisecond)
	enabler := NewTrafficEnabler(opts).(*trafficEnabler)
	require.Error(t, enabler.Init())
	defer enabler.Close()

	disabler := NewTrafficDisabler(opts)
	require.Error(t, disabler.Init())
	defer disabler.Close()
}

func TestTrafficControllerWithInitialKVValue(t *testing.T) {
	defer leaktest.Check(t)()

	store := mem.NewStore()
	key := "testKey"
	_, err := store.Set(key, &commonpb.BoolProto{Value: true})
	require.NoError(t, err)

	opts := NewTrafficControlOptions().
		SetStore(store).
		SetRuntimeKey(key).
		SetInitTimeout(200 * time.Millisecond)
	enabler := NewTrafficEnabler(opts).(*trafficEnabler)
	require.NoError(t, enabler.Init())
	defer enabler.Close()

	disabler := NewTrafficDisabler(opts)
	require.NoError(t, disabler.Init())
	defer disabler.Close()

	require.True(t, enabler.enabled.Load())
	require.True(t, enabler.Allow())
	require.False(t, disabler.Allow())

	_, err = store.Set(key, &commonpb.BoolProto{Value: false})
	require.NoError(t, err)

	for enabler.enabled.Load() {
		time.Sleep(100 * time.Millisecond)
	}
	require.False(t, enabler.Allow())
	for !disabler.Allow() {
		time.Sleep(100 * time.Millisecond)
	}
	require.True(t, disabler.Allow())

	_, err = store.Set(key, &commonpb.BoolProto{Value: true})
	require.NoError(t, err)

	for !enabler.enabled.Load() {
		time.Sleep(100 * time.Millisecond)
	}
	require.True(t, enabler.Allow())
	for disabler.Allow() {
		time.Sleep(100 * time.Millisecond)
	}
	require.False(t, disabler.Allow())
}