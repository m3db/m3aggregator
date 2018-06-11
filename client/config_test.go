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

package client

import (
	"testing"
	"time"

	"github.com/m3db/m3aggregator/sharding"
	m3clusterclient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

var (
	testClientConfig = `
placementKV:
  zone: testZone
  environment: testEnvironment
  namespace: testNamespace
placementWatcher:
  key: testWatchKey
  initWatchTimeout: 15s
hashType: murmur32
shardCutoverWarmupDuration: 10m
shardCutoffLingerDuration: 1m
encoder:
  initBufferSize: 100
  maxMessageSize: 50000000
  bytesPool:
    buckets:
      - capacity: 16
        count: 10
      - capacity: 32
        count: 20
    watermark:
      low: 0.001
      high: 0.01
flushSize: 1440
maxTimerBatchSize: 140
queueSize: 1000
queueDropType: oldest
connection:
  connectionTimeout: 1s
  connectionKeepAlive: true
  writeTimeout: 1s
  initReconnectThreshold: 2
  maxReconnectThreshold: 5000
  reconnectThresholdMultiplier: 2
  maxReconnectDuration: 1m
`
)

func TestConfigUnmarshal(t *testing.T) {
	var cfg Configuration
	require.NoError(t, yaml.Unmarshal([]byte(testClientConfig), &cfg))

	require.Equal(t, "testZone", cfg.PlacementKV.Zone)
	require.Equal(t, "testEnvironment", cfg.PlacementKV.Environment)
	require.Equal(t, "testNamespace", cfg.PlacementKV.Namespace)
	require.Equal(t, "testWatchKey", cfg.PlacementWatcher.Key)
	require.Equal(t, 15*time.Second, cfg.PlacementWatcher.InitWatchTimeout)
	require.Equal(t, sharding.Murmur32Hash, *cfg.HashType)
	require.Equal(t, 10*time.Minute, *cfg.ShardCutoverWarmupDuration)
	require.Equal(t, time.Minute, *cfg.ShardCutoffLingerDuration)
	require.Equal(t, 100, *cfg.Encoder.InitBufferSize)
	require.Equal(t, 50000000, *cfg.Encoder.MaxMessageSize)
	require.Equal(t, []pool.BucketConfiguration{
		{Count: 10, Capacity: 16},
		{Count: 20, Capacity: 32},
	}, cfg.Encoder.BytesPool.Buckets)
	require.Equal(t, 0.001, cfg.Encoder.BytesPool.Watermark.RefillLowWatermark)
	require.Equal(t, 0.01, cfg.Encoder.BytesPool.Watermark.RefillHighWatermark)
	require.Equal(t, 1440, cfg.FlushSize)
	require.Equal(t, 140, cfg.MaxTimerBatchSize)
	require.Equal(t, 1000, cfg.QueueSize)
	require.Equal(t, DropOldest, *cfg.QueueDropType)
	require.Equal(t, time.Second, cfg.Connection.ConnectionTimeout)
	require.Equal(t, true, *cfg.Connection.ConnectionKeepAlive)
	require.Equal(t, time.Second, cfg.Connection.WriteTimeout)
	require.Equal(t, 2, cfg.Connection.InitReconnectThreshold)
	require.Equal(t, 5000, cfg.Connection.MaxReconnectThreshold)
	require.Equal(t, 2, cfg.Connection.ReconnectThresholdMultiplier)
	require.Equal(t, time.Minute, *cfg.Connection.MaxReconnectDuration)
}

func TestNewClientOptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var cfg Configuration
	require.NoError(t, yaml.Unmarshal([]byte(testClientConfig), &cfg))

	expectedKvOpts := kv.NewOverrideOptions().
		SetZone("testZone").
		SetEnvironment("testEnvironment").
		SetNamespace("testNamespace")
	store := mem.NewStore()
	kvClient := m3clusterclient.NewMockClient(ctrl)
	kvClient.EXPECT().Store(expectedKvOpts).Return(store, nil)
	clockOpts := clock.NewOptions()
	instrumentOpts := instrument.NewOptions()
	opts, err := cfg.newClientOptions(kvClient, clockOpts, instrumentOpts)
	require.NoError(t, err)

	// Verify the constructed options match expectations.
	require.True(t, instrumentOpts == opts.InstrumentOptions())
	require.Equal(t, 100, opts.EncoderOptions().InitBufferSize())
	require.Equal(t, 50000000, opts.EncoderOptions().MaxMessageSize())
	require.NotNil(t, opts.EncoderOptions().BytesPool())
	require.NotNil(t, opts.ShardFn())
	require.Equal(t, "testWatchKey", opts.StagedPlacementWatcherOptions().StagedPlacementKey())
	require.True(t, store == opts.StagedPlacementWatcherOptions().StagedPlacementStore())
	require.Equal(t, 10*time.Minute, opts.ShardCutoverWarmupDuration())
	require.Equal(t, time.Minute, opts.ShardCutoffLingerDuration())
	require.Equal(t, 1440, opts.FlushSize())
	require.Equal(t, 140, opts.MaxTimerBatchSize())
	require.Equal(t, 140, opts.MaxTimerBatchSize())
	require.Equal(t, DropOldest, opts.QueueDropType())
	require.Equal(t, time.Second, opts.ConnectionOptions().ConnectionTimeout())
	require.Equal(t, true, opts.ConnectionOptions().ConnectionKeepAlive())
	require.Equal(t, time.Second, opts.ConnectionOptions().WriteTimeout())
	require.Equal(t, 2, opts.ConnectionOptions().InitReconnectThreshold())
	require.Equal(t, 5000, opts.ConnectionOptions().MaxReconnectThreshold())
	require.Equal(t, 2, opts.ConnectionOptions().ReconnectThresholdMultiplier())
	require.Equal(t, time.Minute, opts.ConnectionOptions().MaxReconnectDuration())
}
