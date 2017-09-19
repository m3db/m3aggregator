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
	errUnknownFlushHandlerType           = errors.New("unknown flush handler type")
	errNoForwardHandlerConfiguration     = errors.New("no forward handler configuration")
	errNoPartitionedHandlerConfiguration = errors.New("no partitioned handler configuration")
	errNoBroadcastHandlerConfiguration   = errors.New("no broadcast handler configuration")
)

// FlushHandlerConfiguration contains configuration for flushing metrics.
type FlushHandlerConfiguration struct {
	// Flushing handler type.
	Type Type `yaml:"type"`

	// Forward handler configuration.
	Forward *forwardHandlerConfiguration `yaml:"forward"`

	// Partitioned handler configuration.
	Partitioned *partitionedHandlerConfiguration `yaml:"partitioned"`

	// Forward handler configuration.
	Broadcast *broadcastHandlerConfiguration `yaml:"broadcast"`
}

// NewHandler creates a new flush handler
func (c *FlushHandlerConfiguration) NewHandler(
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
		scope = scope.SubScope("forward").Tagged(map[string]string{"forward-target": c.Forward.Name})
		logger := iOpts.Logger().WithFields(log.NewField("forward-target", c.Forward.Name))
		return c.Forward.NewHandler(iOpts.SetMetricsScope(scope).SetLogger(logger))
	case partitionedType:
		if c.Partitioned == nil {
			return nil, errNoPartitionedHandlerConfiguration
		}
		scope = scope.SubScope("partitioned").Tagged(map[string]string{"partitioned-backend": c.Partitioned.Name})
		logger := iOpts.Logger().WithFields(log.NewField("partitioned-backend", c.Partitioned.Name))
		return c.Partitioned.NewHandler(iOpts.SetMetricsScope(scope).SetLogger(logger))
	case broadcastType:
		if c.Broadcast == nil {
			return nil, errNoBroadcastHandlerConfiguration
		}
		scope = scope.SubScope("broadcast")
		return c.Broadcast.NewHandler(iOpts.SetMetricsScope(scope))
	default:
		return nil, errUnknownFlushHandlerType
	}
}

type broadcastHandlerConfiguration struct {
	// Broadcast target handlers.
	Handlers []FlushHandlerConfiguration `yaml:"handlers" validate:"nonzero"`
}

func (c *broadcastHandlerConfiguration) NewHandler(
	iOpts instrument.Options,
) (aggregator.Handler, error) {
	handlers := make([]aggregator.Handler, 0, len(c.Handlers))
	for _, cfg := range c.Handlers {
		handler, err := cfg.NewHandler(iOpts)
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

type partitionedHandlerConfiguration struct {
	// Name of the partitioned backend.
	Name string `yaml:"name"`

	// Backend server partitions.
	Partitions []BackendServerPartition

	// Total queue size across all partitions.
	QueueSize int `yaml:"queueSize"`

	// Connection configuration.
	Connection connectionConfiguration `yaml:"connection"`
}

func (c *partitionedHandlerConfiguration) NewHandler(
	instrumentOpts instrument.Options,
) (aggregator.Handler, error) {
	if err := c.validatePartitions(); err != nil {
		return nil, err
	}
	connectionOpts := c.Connection.NewConnectionOptions(instrumentOpts.MetricsScope())
	opts := NewOptions().
		SetInstrumentOptions(instrumentOpts).
		SetConnectionOptions(connectionOpts)
	if c.QueueSize != 0 {
		opts = opts.SetQueueSize(c.QueueSize)
	}
	return NewPartitionedHandler(c.Partitions, opts)
}

// validatePartitions ensures a single partition isn't present multiple times, and
// that a single server isn't assigned to multiple partition ranges.
func (c *partitionedHandlerConfiguration) validatePartitions() error {
	var (
		serversAssigned    = make(map[string]struct{})
		partitionsAssigned = make(map[int]struct{})
	)
	for _, partitions := range c.Partitions {
		// Make sure we have a deterministic ordering.
		sortedPartitions := make([]int, 0, len(partitions.Range))
		for partition := range partitions.Range {
			sortedPartitions = append(sortedPartitions, int(partition))
		}
		sort.Ints(sortedPartitions)

		for _, partition := range sortedPartitions {
			if _, partitionAlreadyAssigned := partitionsAssigned[partition]; partitionAlreadyAssigned {
				return fmt.Errorf("partition %d is present in multiple ranges", partition)
			}
			partitionsAssigned[partition] = struct{}{}
		}

		for _, server := range partitions.Servers {
			if _, serverAlreadyAssigned := serversAssigned[server]; serverAlreadyAssigned {
				return fmt.Errorf("server %s is present in multiple ranges", server)
			}
			serversAssigned[server] = struct{}{}
		}
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
