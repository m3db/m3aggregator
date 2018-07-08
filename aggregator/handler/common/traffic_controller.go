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
	"fmt"
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

// TrafficController controls traffic.
type TrafficController interface {
	// Allow returns true if traffic is allowed.
	Allow() bool

	// Init initializes the traffic controller to watch the runtime updates.
	Init() error

	// Close closes the traffic controller.
	Close()
}

// TrafficControllerType defines the type of the traffic controller.
type TrafficControllerType string

// Supported types.
var (
	Enabler  TrafficControllerType = "enabler"
	Disabler TrafficControllerType = "disabler"

	validTypes = []TrafficControllerType{
		Enabler,
		Disabler,
	}
)

// UnmarshalYAML unmarshals TrafficControllerType from yaml.
func (t *TrafficControllerType) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	var validStrings []string
	for _, validType := range validTypes {
		validString := string(validType)
		if validString == str {
			*t = validType
			return nil
		}
		validStrings = append(validStrings, validString)
	}

	return fmt.Errorf("invalid traffic controller type %s, valid types are: %v", str, validStrings)
}

// TrafficControllerConfiguration configures the traffic controller.
type TrafficControllerConfiguration struct {
	Type         TrafficControllerType `yaml:"type"`
	DefaultValue bool                  `yaml:"defaultValue" validate:"nonzero"`
	RuntimeKey   string                `yaml:"runtimeKey" validate:"nonzero"`
	InitTimeout  *time.Duration        `yaml:"initTimeout"`
}

// NewTrafficController creates a new traffic controller.
func (c *TrafficControllerConfiguration) NewTrafficController(
	store kv.Store,
	instrumentOpts instrument.Options,
) (TrafficController, error) {
	opts := NewTrafficControlOptions().
		SetStore(store).
		SetDefaultValue(c.DefaultValue).
		SetRuntimeKey(c.RuntimeKey).
		SetInstrumentOptions(instrumentOpts)
	if c.InitTimeout != nil {
		opts = opts.SetInitTimeout(*c.InitTimeout)
	}
	var tc TrafficController
	if c.Type == Enabler {
		tc = NewTrafficEnabler(opts)
	} else {
		tc = NewTrafficDisabler(opts)
	}
	if err := tc.Init(); err != nil {
		return nil, err
	}
	return tc, nil
}

// TrafficControlOptions configurates the traffic control options.
type TrafficControlOptions interface {
	// SetStore sets the kv store.
	SetStore(store kv.Store) TrafficControlOptions

	// Store returns the kv store.
	Store() kv.Store

	// SetDefaultValue sets the default value.
	SetDefaultValue(value bool) TrafficControlOptions

	// DefaultValue returns the default value.
	DefaultValue() bool

	// SetRuntimeKey sets the runtime enable key,
	// which will override the default enabled value when present.
	SetRuntimeKey(value string) TrafficControlOptions

	// RuntimeKey returns the runtime enable key,
	// which will override the default enabled value when present.
	RuntimeKey() string

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
	store          kv.Store
	defaultValue   bool
	runtimeKey     string
	initTimeout    time.Duration
	instrumentOpts instrument.Options
}

// NewTrafficControlOptions creats a new TrafficControlOptions.
func NewTrafficControlOptions() TrafficControlOptions {
	return &trafficControlOptions{
		defaultValue:   defaultDefaultDisabled,
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

func (opts *trafficControlOptions) SetDefaultValue(value bool) TrafficControlOptions {
	o := *opts
	o.defaultValue = value
	return &o
}

func (opts *trafficControlOptions) DefaultValue() bool {
	return opts.defaultValue
}

func (opts *trafficControlOptions) SetRuntimeKey(value string) TrafficControlOptions {
	o := *opts
	o.runtimeKey = value
	return &o
}

func (opts *trafficControlOptions) RuntimeKey() string {
	return opts.runtimeKey
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

type trafficEnabler struct {
	enabled *atomic.Bool
	value   watch.Value
	opts    TrafficControlOptions
}

// NewTrafficEnabler creates a new traffic controller.
func NewTrafficEnabler(opts TrafficControlOptions) TrafficController {
	enabled := atomic.NewBool(opts.DefaultValue())
	iOpts := opts.InstrumentOptions()
	newUpdatableFn := func() (watch.Updatable, error) {
		w, err := opts.Store().Watch(opts.RuntimeKey())
		return w, err
	}
	getFn := func(updatable watch.Updatable) (interface{}, error) {
		return updatable.(kv.ValueWatch).Get(), nil
	}
	processFn := func(update interface{}) error {
		b, err := util.BoolFromValue(
			update.(kv.Value),
			opts.RuntimeKey(),
			opts.DefaultValue(),
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
	return &trafficEnabler{
		enabled: enabled,
		value:   watch.NewValue(vOptions),
		opts:    opts,
	}
}

func (c *trafficEnabler) Init() error {
	err := c.value.Watch()
	if err == nil {
		return nil
	}
	if _, ok := err.(watch.CreateWatchError); ok {
		return err
	}
	return nil
}

func (c *trafficEnabler) Close() {
	c.value.Unwatch()
}

func (c *trafficEnabler) Allow() bool {
	return c.enabled.Load()
}

type trafficDisabler struct {
	TrafficController
}

// NewTrafficDisabler creates a new traffic disabler.
func NewTrafficDisabler(opts TrafficControlOptions) TrafficController {
	return &trafficDisabler{
		TrafficController: NewTrafficEnabler(opts),
	}
}

func (c *trafficDisabler) Allow() bool {
	return !c.TrafficController.Allow()
}
