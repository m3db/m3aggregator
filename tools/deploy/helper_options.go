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

package deploy

import (
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/retry"
	"github.com/m3db/m3x/sync"
)

const (
	defaultSettleDurationBetweenSteps = time.Minute
	defaultHelperWorkerPoolSize       = 16
)

// HelperOptions provide a set of options for the deployment helper.
type HelperOptions interface {
	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) HelperOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetManager sets the deployment manager.
	SetManager(value Manager) HelperOptions

	// Manager returns the deployment manager.
	Manager() Manager

	// SetPlanner sets the deployment planner.
	SetPlanner(value Planner) HelperOptions

	// Planner returns the deployment planner.
	Planner() Planner

	// SetAggregatorClient sets the aggregator client.
	SetAggregatorClient(value AggregatorClient) HelperOptions

	// AggregatorClient returns the aggregator client.
	AggregatorClient() AggregatorClient

	// SetKVStore sets the kv store.
	SetKVStore(value kv.Store) HelperOptions

	// KVStore returns the kv store.
	KVStore() kv.Store

	// SetRetrier sets the retrier.
	SetRetrier(value xretry.Retrier) HelperOptions

	// Retrier returns the retrier.
	Retrier() xretry.Retrier

	// SetWorkerPool sets the worker pool.
	SetWorkerPool(value xsync.WorkerPool) HelperOptions

	// WorkerPool returns the worker pool.
	WorkerPool() xsync.WorkerPool

	// SetStagedPlacementWatcherOptions sets the staged placement watcher options.
	SetStagedPlacementWatcherOptions(value services.StagedPlacementWatcherOptions) HelperOptions

	// StagedPlacementWatcherOptions returns the staged placement watcher options.
	StagedPlacementWatcherOptions() services.StagedPlacementWatcherOptions

	// SetSettleDurationBetweenSteps sets the settlement duration between consecutive steps.
	SetSettleDurationBetweenSteps(value time.Duration) HelperOptions

	// SettleDurationBetweenSteps returns the settlement duration between consecutive steps.
	SettleDurationBetweenSteps() time.Duration
}

type helperOptions struct {
	instrumentOpts instrument.Options
	manager        Manager
	planner        Planner
	client         AggregatorClient
	store          kv.Store
	retrier        xretry.Retrier
	workerPool     xsync.WorkerPool
	watcherOpts    services.StagedPlacementWatcherOptions
	settleDuration time.Duration
}

// NewHelperOptions create a set of deployment helper options.
func NewHelperOptions() HelperOptions {
	workers := xsync.NewWorkerPool(defaultHelperWorkerPoolSize)
	workers.Init()
	return &helperOptions{
		instrumentOpts: instrument.NewOptions(),
		retrier:        xretry.NewRetrier(xretry.NewOptions()),
		workerPool:     workers,
		settleDuration: defaultSettleDurationBetweenSteps,
	}
}

func (o *helperOptions) SetInstrumentOptions(value instrument.Options) HelperOptions {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *helperOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *helperOptions) SetManager(value Manager) HelperOptions {
	opts := *o
	opts.manager = value
	return &opts
}

func (o *helperOptions) Manager() Manager {
	return o.manager
}

func (o *helperOptions) SetPlanner(value Planner) HelperOptions {
	opts := *o
	opts.planner = value
	return &opts
}

func (o *helperOptions) Planner() Planner {
	return o.planner
}

func (o *helperOptions) SetAggregatorClient(value AggregatorClient) HelperOptions {
	opts := *o
	opts.client = value
	return &opts
}

func (o *helperOptions) AggregatorClient() AggregatorClient {
	return o.client
}

func (o *helperOptions) SetKVStore(value kv.Store) HelperOptions {
	opts := *o
	opts.store = value
	return &opts
}

func (o *helperOptions) KVStore() kv.Store {
	return o.store
}

func (o *helperOptions) SetRetrier(value xretry.Retrier) HelperOptions {
	opts := *o
	opts.retrier = value
	return &opts
}

func (o *helperOptions) Retrier() xretry.Retrier {
	return o.retrier
}

func (o *helperOptions) SetWorkerPool(value xsync.WorkerPool) HelperOptions {
	opts := *o
	opts.workerPool = value
	return &opts
}

func (o *helperOptions) WorkerPool() xsync.WorkerPool {
	return o.workerPool
}

func (o *helperOptions) SetStagedPlacementWatcherOptions(value services.StagedPlacementWatcherOptions) HelperOptions {
	opts := *o
	opts.watcherOpts = value
	return &opts
}

func (o *helperOptions) StagedPlacementWatcherOptions() services.StagedPlacementWatcherOptions {
	return o.watcherOpts
}

func (o *helperOptions) SetSettleDurationBetweenSteps(value time.Duration) HelperOptions {
	opts := *o
	opts.settleDuration = value
	return &opts
}

func (o *helperOptions) SettleDurationBetweenSteps() time.Duration {
	return o.settleDuration
}
