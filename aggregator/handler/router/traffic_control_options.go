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

package router

import (
	"fmt"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3x/instrument"
)

// TrafficControllerType defines the type of the traffic controller.
type TrafficControllerType string

var (
	// TrafficEnabler enables the traffic when the runtime value equals to true.
	TrafficEnabler TrafficControllerType = "trafficEnabler"
	// TrafficDisabler disables the traffic when the runtime value equals to true.
	TrafficDisabler TrafficControllerType = "trafficDisabler"

	validTypes = []TrafficControllerType{
		TrafficEnabler,
		TrafficDisabler,
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
	DefaultValue bool                  `yaml:"defaultValue"`
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
	if c.Type == TrafficEnabler {
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
		initTimeout:    defaultInitTimeout,
		instrumentOpts: instrument.NewOptions(),
	}
}

func (o *trafficControlOptions) SetStore(store kv.Store) TrafficControlOptions {
	opts := *o
	opts.store = store
	return &opts
}

func (o *trafficControlOptions) Store() kv.Store {
	return o.store
}

func (o *trafficControlOptions) SetDefaultValue(value bool) TrafficControlOptions {
	opts := *o
	opts.defaultValue = value
	return &opts
}

func (o *trafficControlOptions) DefaultValue() bool {
	return o.defaultValue
}

func (o *trafficControlOptions) SetRuntimeKey(value string) TrafficControlOptions {
	opts := *o
	opts.runtimeKey = value
	return &opts
}

func (o *trafficControlOptions) RuntimeKey() string {
	return o.runtimeKey
}

func (o *trafficControlOptions) SetInitTimeout(value time.Duration) TrafficControlOptions {
	opts := *o
	opts.initTimeout = value
	return &opts
}

func (o *trafficControlOptions) InitTimeout() time.Duration {
	return o.initTimeout
}

func (o *trafficControlOptions) SetInstrumentOptions(value instrument.Options) TrafficControlOptions {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *trafficControlOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}
