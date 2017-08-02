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

// DeploymentHelperOptions provide a set of options for the deployment helper.
type DeploymentHelperOptions interface {
	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) DeploymentHelperOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetDeploymentManager sets the deployment manager.
	SetDeploymentManager(value DeploymentManager) DeploymentHelperOptions

	// DeploymentManager returns the deployment manager.
	DeploymentManager() DeploymentManager

	// SetDeploymentPlanner sets the deployment planner.
	SetDeploymentPlanner(value DeploymentPlanner) DeploymentHelperOptions

	// DeploymentPlanner returns the deployment planner.
	DeploymentPlanner() DeploymentPlanner

	// SetAggregatorClient sets the aggregator client.
	SetAggregatorClient(value AggregatorClient) DeploymentHelperOptions

	// AggregatorClient returns the aggregator client.
	AggregatorClient() AggregatorClient

	// SetKVStore sets the kv store.
	SetKVStore(value kv.Store) DeploymentHelperOptions

	// KVStore returns the kv store.
	KVStore() kv.Store

	// SetRetrier sets the retrier.
	SetRetrier(value xretry.Retrier) DeploymentHelperOptions

	// Retrier returns the retrier.
	Retrier() xretry.Retrier

	// SetWorkerPool sets the worker pool.
	SetWorkerPool(value xsync.WorkerPool) DeploymentHelperOptions

	// WorkerPool returns the worker pool.
	WorkerPool() xsync.WorkerPool

	// SetStagedPlacementWatcherOptions sets the staged placement watcher options.
	SetStagedPlacementWatcherOptions(value services.StagedPlacementWatcherOptions) DeploymentHelperOptions

	// StagedPlacementWatcherOptions returns the staged placement watcher options.
	StagedPlacementWatcherOptions() services.StagedPlacementWatcherOptions

	// SetSettleDurationBetweenSteps sets the settlement duration between consecutive steps.
	SetSettleDurationBetweenSteps(value time.Duration) DeploymentHelperOptions

	// SettleDurationBetweenSteps returns the settlement duration between consecutive steps.
	SettleDurationBetweenSteps() time.Duration
}

type deploymentHelperOptions struct {
	instrumentOpts instrument.Options
	manager        DeploymentManager
	planner        DeploymentPlanner
	client         AggregatorClient
	store          kv.Store
	retrier        xretry.Retrier
	workerPool     xsync.WorkerPool
	watcherOpts    services.StagedPlacementWatcherOptions
	settleDuration time.Duration
}

// NewDeploymentHelperOptions create a set of deployment helper options.
func NewDeploymentHelperOptions() DeploymentHelperOptions {
	workers := xsync.NewWorkerPool(defaultHelperWorkerPoolSize)
	workers.Init()
	return &deploymentHelperOptions{
		instrumentOpts: instrument.NewOptions(),
		retrier:        xretry.NewRetrier(xretry.NewOptions()),
		workerPool:     workers,
		settleDuration: defaultSettleDurationBetweenSteps,
	}
}

func (o *deploymentHelperOptions) SetInstrumentOptions(value instrument.Options) DeploymentHelperOptions {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *deploymentHelperOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *deploymentHelperOptions) SetDeploymentManager(value DeploymentManager) DeploymentHelperOptions {
	opts := *o
	opts.manager = value
	return &opts
}

func (o *deploymentHelperOptions) DeploymentManager() DeploymentManager {
	return o.manager
}

func (o *deploymentHelperOptions) SetDeploymentPlanner(value DeploymentPlanner) DeploymentHelperOptions {
	opts := *o
	opts.planner = value
	return &opts
}

func (o *deploymentHelperOptions) DeploymentPlanner() DeploymentPlanner {
	return o.planner
}

func (o *deploymentHelperOptions) SetAggregatorClient(value AggregatorClient) DeploymentHelperOptions {
	opts := *o
	opts.client = value
	return &opts
}

func (o *deploymentHelperOptions) AggregatorClient() AggregatorClient {
	return o.client
}

func (o *deploymentHelperOptions) SetKVStore(value kv.Store) DeploymentHelperOptions {
	opts := *o
	opts.store = value
	return &opts
}

func (o *deploymentHelperOptions) KVStore() kv.Store {
	return o.store
}

func (o *deploymentHelperOptions) SetRetrier(value xretry.Retrier) DeploymentHelperOptions {
	opts := *o
	opts.retrier = value
	return &opts
}

func (o *deploymentHelperOptions) Retrier() xretry.Retrier {
	return o.retrier
}

func (o *deploymentHelperOptions) SetWorkerPool(value xsync.WorkerPool) DeploymentHelperOptions {
	opts := *o
	opts.workerPool = value
	return &opts
}

func (o *deploymentHelperOptions) WorkerPool() xsync.WorkerPool {
	return o.workerPool
}

func (o *deploymentHelperOptions) SetStagedPlacementWatcherOptions(value services.StagedPlacementWatcherOptions) DeploymentHelperOptions {
	opts := *o
	opts.watcherOpts = value
	return &opts
}

func (o *deploymentHelperOptions) StagedPlacementWatcherOptions() services.StagedPlacementWatcherOptions {
	return o.watcherOpts
}

func (o *deploymentHelperOptions) SetSettleDurationBetweenSteps(value time.Duration) DeploymentHelperOptions {
	opts := *o
	opts.settleDuration = value
	return &opts
}

func (o *deploymentHelperOptions) SettleDurationBetweenSteps() time.Duration {
	return o.settleDuration
}
