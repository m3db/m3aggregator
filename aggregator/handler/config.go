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
	"time"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/retry"
)

var (
	errUnknownFlushHandlerType            = errors.New("unknown flush handler type")
	errNoForwardHandlerConfiguration      = errors.New("no forward flush configuration")
	errEmptyBroadcastHandlerConfiguration = errors.New("emtpy broadcast flush configuration")
)

// FlushHandlerConfiguration contains configuration for flushing metrics.
type FlushHandlerConfiguration struct {
	// Flushing handler type.
	Type string `yaml:"type" validate:"regexp=(^blackhole$|^logging$|^forward$|^broadcast$)"`

	// Forward handler configuration.
	Forward *forwardHandlerConfiguration `yaml:"forward"`

	// Forward handler configuration.
	Broadcast []broadcastHandlerConfiguration `yaml:"broadcast"`
}

// NewHandler creates a new flush handler
func (c *FlushHandlerConfiguration) NewHandler(
	iOpts instrument.Options,
) (aggregator.Handler, error) {
	scope := iOpts.MetricsScope()
	switch Type(c.Type) {
	case BlackholeHandler:
		return NewBlackholeHandler(), nil
	case LoggingHandler:
		scope = scope.SubScope("logging").Tagged(map[string]string{"handler": "logging"})
		return NewLoggingHandler(iOpts.SetMetricsScope(scope)), nil
	case ForwardHandler:
		if c.Forward == nil {
			return nil, errNoForwardHandlerConfiguration
		}
		scope = scope.SubScope("forward").Tagged(map[string]string{"handler": "forward", "target": c.Forward.Name})
		logger := iOpts.Logger().WithFields(xlog.NewLogField("forward-target", c.Forward.Name))
		return c.Forward.NewHandler(iOpts.SetMetricsScope(scope).SetLogger(logger))
	case Broadcast:
		scope = scope.Tagged(map[string]string{"handler": "broadcast"})
		return newBroadcastHandler(c.Broadcast, iOpts.SetMetricsScope(scope))
	default:
		return nil, errUnknownFlushHandlerType
	}
}

func newBroadcastHandler(cfgs []broadcastHandlerConfiguration, iopts instrument.Options) (aggregator.Handler, error) {
	if len(cfgs) == 0 {
		return nil, errEmptyBroadcastHandlerConfiguration
	}

	handlers := make([]aggregator.Handler, len(cfgs))
	for i, cfg := range cfgs {
		h, err := cfg.NewHandler(iopts)
		if err != nil {
			return nil, err
		}
		handlers[i] = h
	}
	return NewBroadcastHandler(handlers), nil
}

type broadcastHandlerConfiguration struct {
	// Flushing handler type.
	Type string `yaml:"type" validate:"regexp=(^blackhole$|^logging$|^forward$)"`

	// Forward handler configuration.
	Forward *forwardHandlerConfiguration `yaml:"forward"`
}

func (c broadcastHandlerConfiguration) NewHandler(iOpts instrument.Options) (aggregator.Handler, error) {
	scope := iOpts.MetricsScope()
	switch Type(c.Type) {
	case BlackholeHandler:
		return NewBlackholeHandler(), nil
	case LoggingHandler:
		scope = scope.SubScope("logging")
		iOpts = iOpts.SetMetricsScope(scope)
		return NewLoggingHandler(iOpts), nil
	case ForwardHandler:
		if c.Forward == nil {
			return nil, errNoForwardHandlerConfiguration
		}
		scope = scope.SubScope("forward").Tagged(map[string]string{"target": c.Forward.Name})
		logger := iOpts.Logger().WithFields(xlog.NewLogField("broadcast-target", c.Forward.Name))
		return c.Forward.NewHandler(iOpts.SetMetricsScope(scope).SetLogger(logger))
	default:
		return nil, errUnknownFlushHandlerType
	}
}

// forwardHandlerConfiguration contains configuration for forward
type forwardHandlerConfiguration struct {
	// Name of the forward target.
	Name string `yaml:"name"`

	// Server address list.
	Servers []string `yaml:"servers"`

	// Buffer queue size.
	QueueSize int `yaml:"queueSize"`

	// Connection timeout.
	ConnectTimeout time.Duration `yaml:"connectTimeout"`

	// Connection keep alive.
	ConnectionKeepAlive *bool `yaml:"connectionKeepAlive"`

	// Connection write timeout.
	ConnectionWriteTimeout time.Duration `yaml:"connectionWriteTimeout"`

	// Reconnect retrier.
	ReconnectRetrier xretry.Configuration `yaml:"reconnect"`
}

func (c *forwardHandlerConfiguration) NewHandler(
	instrumentOpts instrument.Options,
) (aggregator.Handler, error) {
	opts := NewForwardHandlerOptions().SetInstrumentOptions(instrumentOpts)

	if c.QueueSize != 0 {
		opts = opts.SetQueueSize(c.QueueSize)
	}
	if c.ConnectTimeout != 0 {
		opts = opts.SetConnectTimeout(c.ConnectTimeout)
	}
	if c.ConnectionKeepAlive != nil {
		opts = opts.SetConnectionKeepAlive(*c.ConnectionKeepAlive)
	}
	if c.ConnectionWriteTimeout != 0 {
		opts = opts.SetConnectionWriteTimeout(c.ConnectionWriteTimeout)
	}

	scope := instrumentOpts.MetricsScope().SubScope("reconnect")
	retrier := c.ReconnectRetrier.NewRetrier(scope)
	opts = opts.SetReconnectRetrier(retrier)

	return NewForwardHandler(c.Servers, opts)
}
