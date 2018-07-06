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
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/util"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/watch"

	"go.uber.org/atomic"
)

const (
	defaultDefaultDisabled = false
	defaultInitTimeout     = 2 * time.Second
)

// TrafficControlOptions configurates the traffic control options.
type TrafficControlOptions interface {
	// SetStore sets the kv store.
	SetStore(store kv.Store) TrafficControlOptions

	// Store returns the kv store.
	Store() kv.Store

	// SetDefaultEnabled sets the default enabled value.
	SetDefaultEnabled(value bool) TrafficControlOptions

	// DefaultEnabled returns the default enabled value.
	DefaultEnabled() bool

	// SetRuntimeEnableKey sets the runtime enable key,
	// which will override the default enabled value when present.
	SetRuntimeEnableKey(value string) TrafficControlOptions

	// RuntimeEnableKey returns the runtime enable key,
	// which will override the default enabled value when present.
	RuntimeEnableKey() string

	// SetInitTimeout sets the init timeout.
	SetInitTimeout(value time.Duration) TrafficControlOptions

	// InitTimeout returns the init timeout.
	InitTimeout() time.Duration

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) TrafficControlOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options
}

type trafficControlOptions struct {
	store            kv.Store
	defaultEnabled   bool
	runtimeEnableKey string
	initTimeout      time.Duration
	instrumentOpts   instrument.Options
}

// NewTrafficControlOptions creats a new TrafficControlOptions.
func NewTrafficControlOptions() TrafficControlOptions {
	return &trafficControlOptions{
		defaultEnabled: defaultDefaultDisabled,
		initTimeout:    defaultInitTimeout,
		instrumentOpts: instrument.NewOptions(),
	}
}

func (opts *trafficControlOptions) SetStore(store kv.Store) TrafficControlOptions {
	o := *opts
	o.store = store
	return &o
}

func (opts *trafficControlOptions) Store() kv.Store {
	return opts.store
}

func (opts *trafficControlOptions) SetDefaultEnabled(value bool) TrafficControlOptions {
	o := *opts
	o.defaultEnabled = value
	return &o
}

func (opts *trafficControlOptions) DefaultEnabled() bool {
	return opts.defaultEnabled
}

func (opts *trafficControlOptions) SetRuntimeEnableKey(value string) TrafficControlOptions {
	o := *opts
	o.runtimeEnableKey = value
	return &o
}

func (opts *trafficControlOptions) RuntimeEnableKey() string {
	return opts.runtimeEnableKey
}

func (opts *trafficControlOptions) SetInitTimeout(value time.Duration) TrafficControlOptions {
	o := *opts
	o.initTimeout = value
	return &o
}

func (opts *trafficControlOptions) InitTimeout() time.Duration {
	return opts.initTimeout
}

func (opts *trafficControlOptions) SetInstrumentOptions(value instrument.Options) TrafficControlOptions {
	o := *opts
	o.instrumentOpts = value
	return &o
}

func (opts *trafficControlOptions) InstrumentOptions() instrument.Options {
	return opts.instrumentOpts
}

// TrafficController controls if traffic is enabled.
type TrafficController struct {
	enabled *atomic.Bool
	value   watch.Value
	opts    TrafficControlOptions
}

// NewTrafficController creates a new traffic controller.
func NewTrafficController(opts TrafficControlOptions) *TrafficController {
	enabled := atomic.NewBool(opts.DefaultEnabled())
	iOpts := opts.InstrumentOptions()
	newUpdatableFn := func() (watch.Updatable, error) {
		w, err := opts.Store().Watch(opts.RuntimeEnableKey())
		return w, err
	}
	getFn := func(updatable watch.Updatable) (interface{}, error) {
		return updatable.(kv.ValueWatch).Get(), nil
	}
	processFn := func(update interface{}) error {
		b, err := util.BoolFromValue(
			update.(kv.Value),
			opts.RuntimeEnableKey(),
			opts.DefaultEnabled(),
			util.NewOptions().SetLogger(iOpts.Logger()),
		)
		if err != nil {
			return err
		}
		enabled.Store(b)
		return nil
	}
	vOptions := watch.NewOptions().
		SetInitWatchTimeout(opts.InitTimeout()).
		SetInstrumentOptions(iOpts).
		SetNewUpdatableFn(newUpdatableFn).
		SetGetUpdateFn(getFn).
		SetProcessFn(processFn)
	return &TrafficController{
		enabled: enabled,
		value:   watch.NewValue(vOptions),
		opts:    opts,
	}
}

// Init initializes the traffic controller to watch the runtime updates.
func (c *TrafficController) Init() error {
	err := c.value.Watch()
	if err == nil {
		return nil
	}
	if _, ok := err.(watch.CreateWatchError); ok {
		return err
	}
	return nil
}

// Close closes the traffic controller.
func (c *TrafficController) Close() {
	c.value.Unwatch()
}

// Allow returns true if traffic is allowed.
func (c *TrafficController) Allow() bool {
	return c.enabled.Load()
}
