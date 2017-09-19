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

package handler

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/retry"

	"github.com/uber-go/tally"
)

var (
	errUnknownFlushHandlerType         = errors.New("unknown flush handler type")
	errNoForwardHandlerConfiguration   = errors.New("no forward handler configuration")
	errNoShardedHandlerConfiguration   = errors.New("no sharded handler configuration")
	errNoBroadcastHandlerConfiguration = errors.New("no broadcast handler configuration")
)

// FlushHandlerConfiguration contains configuration for flushing metrics.
type FlushHandlerConfiguration struct {
	// Flushing handler type.
	Type Type `yaml:"type"`

	// Forward handler configuration.
	Forward *forwardHandlerConfiguration `yaml:"forward"`

	// Sharded handler configuration.
	Sharded *shardedHandlerConfiguration `yaml:"sharded"`

	// Forward handler configuration.
	Broadcast *broadcastHandlerConfiguration `yaml:"broadcast"`
}

// NewHandler creates a new flush handler
func (c *FlushHandlerConfiguration) NewHandler(
	totalShards int,
	iOpts instrument.Options,
) (aggregator.Handler, error) {
	scope := iOpts.MetricsScope()
	switch c.Type {
	case blackholeType:
		return NewBlackholeHandler(), nil
	case loggingType:
		scope = scope.SubScope("logging")
		return NewLoggingHandler(iOpts.SetMetricsScope(scope)), nil
	case forwardType:
		if c.Forward == nil {
			return nil, errNoForwardHandlerConfiguration
		}
		scope = scope.SubScope("forward").Tagged(map[string]string{"backend": c.Forward.Name})
		logger := iOpts.Logger().WithFields(log.NewField("backend", c.Forward.Name))
		return c.Forward.NewHandler(iOpts.SetMetricsScope(scope).SetLogger(logger))
	case shardedType:
		if c.Sharded == nil {
			return nil, errNoShardedHandlerConfiguration
		}
		scope = scope.SubScope("sharded").Tagged(map[string]string{"backend": c.Sharded.Name})
		logger := iOpts.Logger().WithFields(log.NewField("backend", c.Sharded.Name))
		return c.Sharded.NewHandler(totalShards, iOpts.SetMetricsScope(scope).SetLogger(logger))
	case broadcastType:
		if c.Broadcast == nil {
			return nil, errNoBroadcastHandlerConfiguration
		}
		scope = scope.SubScope("broadcast")
		return c.Broadcast.NewHandler(totalShards, iOpts.SetMetricsScope(scope))
	default:
		return nil, errUnknownFlushHandlerType
	}
}

type broadcastHandlerConfiguration struct {
	// Broadcast target handlers.
	Handlers []FlushHandlerConfiguration `yaml:"handlers" validate:"nonzero"`
}

func (c *broadcastHandlerConfiguration) NewHandler(
	totalShards int,
	iOpts instrument.Options,
) (aggregator.Handler, error) {
	handlers := make([]aggregator.Handler, 0, len(c.Handlers))
	for _, cfg := range c.Handlers {
		handler, err := cfg.NewHandler(totalShards, iOpts)
		if err != nil {
			return nil, err
		}
		handlers = append(handlers, handler)
	}
	return NewBroadcastHandler(handlers), nil
}

type connectionConfiguration struct {
	// Connection timeout.
	ConnectTimeout time.Duration `yaml:"connectTimeout"`

	// Connection keep alive.
	ConnectionKeepAlive *bool `yaml:"connectionKeepAlive"`

	// Connection write timeout.
	ConnectionWriteTimeout time.Duration `yaml:"connectionWriteTimeout"`

	// Reconnect retrier.
	ReconnectRetrier retry.Configuration `yaml:"reconnect"`
}

func (c *connectionConfiguration) NewConnectionOptions(scope tally.Scope) ConnectionOptions {
	opts := NewConnectionOptions()
	if c.ConnectTimeout != 0 {
		opts = opts.SetConnectTimeout(c.ConnectTimeout)
	}
	if c.ConnectionKeepAlive != nil {
		opts = opts.SetConnectionKeepAlive(*c.ConnectionKeepAlive)
	}
	if c.ConnectionWriteTimeout != 0 {
		opts = opts.SetConnectionWriteTimeout(c.ConnectionWriteTimeout)
	}
	reconnectScope := scope.SubScope("reconnect")
	retrier := c.ReconnectRetrier.NewRetrier(reconnectScope)
	opts = opts.SetReconnectRetrier(retrier)
	return opts
}

type shardedHandlerConfiguration struct {
	// Name of the sharded backend.
	Name string `yaml:"name"`

	// Backend server shards.
	Shards []BackendServerShard `yaml:"shards"`

	// Total queue size across all shards.
	QueueSize int `yaml:"queueSize"`

	// Connection configuration.
	Connection connectionConfiguration `yaml:"connection"`

	// Disable validation (dangerous but useful for testing and staging environments).
	DisableValidation bool `yaml:"disableValidation"`
}

func (c *shardedHandlerConfiguration) NewHandler(
	totalShards int,
	instrumentOpts instrument.Options,
) (aggregator.Handler, error) {
	if !c.DisableValidation {
		if err := c.validateShards(totalShards); err != nil {
			return nil, err
		}
	}
	connectionOpts := c.Connection.NewConnectionOptions(instrumentOpts.MetricsScope())
	opts := NewOptions().
		SetInstrumentOptions(instrumentOpts).
		SetConnectionOptions(connectionOpts)
	if c.QueueSize != 0 {
		opts = opts.SetQueueSize(c.QueueSize)
	}
	return NewShardedHandler(c.Shards, totalShards, opts)
}

// validateShards ensures a single shard isn't present multiple times, and
// that a single server isn't assigned to multiple shard ranges.
func (c *shardedHandlerConfiguration) validateShards(totalShards int) error {
	var (
		serversAssigned = make(map[string]struct{})
		shardsAssigned  = make(map[int]struct{})
	)
	for _, shards := range c.Shards {
		// Make sure we have a deterministic ordering.
		sortedShards := make([]int, 0, len(shards.Range))
		for shard := range shards.Range {
			sortedShards = append(sortedShards, int(shard))
		}
		sort.Ints(sortedShards)

		for _, shard := range sortedShards {
			if shard >= totalShards {
				return fmt.Errorf("shard %d exceeds total available shards %d", shard, totalShards)
			}
			if _, shardAlreadyAssigned := shardsAssigned[shard]; shardAlreadyAssigned {
				return fmt.Errorf("shard %d is present in multiple ranges", shard)
			}
			shardsAssigned[shard] = struct{}{}
		}

		for _, server := range shards.Servers {
			if _, serverAlreadyAssigned := serversAssigned[server]; serverAlreadyAssigned {
				return fmt.Errorf("server %s is present in multiple ranges", server)
			}
			serversAssigned[server] = struct{}{}
		}
	}
	if len(shardsAssigned) != totalShards {
		return fmt.Errorf("missing shards; expected %d total received %d",
			totalShards, len(shardsAssigned))
	}
	return nil
}

// forwardHandlerConfiguration contains configuration for forward handler.
type forwardHandlerConfiguration struct {
	// Name of the forward target.
	Name string `yaml:"name"`

	// Server address list.
	Servers []string `yaml:"servers"`

	// Buffer queue size.
	QueueSize int `yaml:"queueSize"`

	// Connection configuration.
	Connection connectionConfiguration `yaml:"connection"`
}

func (c *forwardHandlerConfiguration) NewHandler(
	instrumentOpts instrument.Options,
) (aggregator.Handler, error) {
	connectionOpts := c.Connection.NewConnectionOptions(instrumentOpts.MetricsScope())
	opts := NewOptions().
		SetInstrumentOptions(instrumentOpts).
		SetConnectionOptions(connectionOpts)
	if c.QueueSize != 0 {
		opts = opts.SetQueueSize(c.QueueSize)
	}
	return NewForwardHandler(c.Servers, opts)
}
