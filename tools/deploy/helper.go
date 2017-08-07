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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/retry"
	"github.com/m3db/m3x/sync"
)

var (
	errNoDeploymentProgress = errors.New("no deployment progress")
	errInvalidRevision      = errors.New("invalid revision")
)

// Mode is the deployment mode.
type Mode int

// A list of supported deployment modes.
const (
	DryRunMode Mode = iota
	ForceMode
)

// Helper is a helper class handling deployments.
type Helper interface {
	// Deploy deploys a target revision to the instances in the placement.
	Deploy(revision string, mode Mode) error
}

// TODO(xichen): disable deployment while another is ongoing.
type helper struct {
	logger             xlog.Logger
	planner            planner
	client             aggregatorClient
	mgr                Manager
	store              kv.Store
	retrier            xretry.Retrier
	foreverRetrier     xretry.Retrier
	workers            xsync.WorkerPool
	toPlacementIDFn    ToPlacementInstanceIDFn
	toAPIEndpointFn    ToAPIEndpointFn
	placementWatcher   services.StagedPlacementWatcher
	settleBetweenSteps time.Duration
}

// NewHelper creates a new deployment helper.
func NewHelper(opts HelperOptions) (Helper, error) {
	client := newAggregatorClient(opts.HTTPClient())
	planner := newPlanner(client, opts.PlannerOptions())
	placementWatcherOpts := opts.StagedPlacementWatcherOptions()
	placementWatcher := placement.NewStagedPlacementWatcher(placementWatcherOpts)
	if err := placementWatcher.Watch(); err != nil {
		return nil, err
	}
	retryOpts := opts.RetryOptions()
	retrier := xretry.NewRetrier(retryOpts)
	foreverRetrier := xretry.NewRetrier(retryOpts.SetForever(true))
	return helper{
		logger:             opts.InstrumentOptions().Logger(),
		planner:            planner,
		client:             client,
		mgr:                opts.Manager(),
		store:              opts.KVStore(),
		retrier:            retrier,
		foreverRetrier:     foreverRetrier,
		workers:            opts.WorkerPool(),
		toPlacementIDFn:    opts.ToPlacementInstanceIDFn(),
		toAPIEndpointFn:    opts.ToAPIEndpointFn(),
		placementWatcher:   placementWatcher,
		settleBetweenSteps: opts.SettleDurationBetweenSteps(),
	}, nil
}

func (h helper) Deploy(revision string, mode Mode) error {
	if revision == "" {
		return errInvalidRevision
	}
	all, err := h.allInstanceMetadatas()
	if err != nil {
		return fmt.Errorf("unable to get all instance metadatas: %v", err)
	}
	filtered := filterByRevision(all, revision)
	plan, err := h.planner.GeneratePlan(filtered, all)
	if err != nil {
		return fmt.Errorf("unable to generate deployment plan: %v", err)
	}

	// If in dry run mode, log the generated deployment plan and return.
	if mode == DryRunMode {
		h.logger.Infof("Generated deployment plan: %+v", plan)
		return nil
	}

	if err = h.execute(plan, revision, filtered, all); err != nil {
		return fmt.Errorf("unable to execute deployment plan: %v", err)
	}
	return nil
}

// Close closes the deployment helper.
func (h helper) Close() error {
	if h.placementWatcher == nil {
		return nil
	}
	return h.placementWatcher.Unwatch()
}

func (h helper) execute(
	plan deploymentPlan,
	revision string,
	toDeploy, all instanceMetadatas,
) error {
	numSteps := len(plan.Steps)
	for i, step := range plan.Steps {
		h.logger.Infof("deploying step %d of %d", i+1, numSteps)
		if err := h.executeStep(step, revision, toDeploy, all); err != nil {
			return err
		}
		if i < numSteps-1 && h.settleBetweenSteps > 0 {
			h.logger.Infof("waiting settle duration after step: %s", h.settleBetweenSteps.String())
			time.Sleep(h.settleBetweenSteps)
		}
		h.logger.Infof("deploying step %d succeeded", i+1)
	}
	return nil
}

func (h helper) executeStep(
	step deploymentStep,
	revision string,
	toDeploy, all instanceMetadatas,
) error {
	h.logger.Infof("waiting until safe to deploy for step %v", step)
	if err := h.waitUntilSafe(all); err != nil {
		return err
	}

	h.logger.Infof("waiting until all targets are validated for step %v", step)
	if err := h.validate(step.Targets); err != nil {
		return err
	}

	h.logger.Infof("waiting until all targets have resigned for step %v", step)
	if err := h.resign(step.Targets); err != nil {
		return err
	}

	h.logger.Infof("beginning to deploy instances for step %v", step)
	if err := h.deploy(step.Targets, revision); err != nil {
		return err
	}

	h.logger.Infof("deployment started, waiting for progress: %v", step)
	if err := h.waitUntilProgressing(toDeploy, revision); err != nil {
		return err
	}

	h.logger.Infof("deployment progressed, waiting for completion: %v", step)
	if err := h.waitUntilSafe(all); err != nil {
		return err
	}
	return nil
}

func (h helper) waitUntilSafe(instances instanceMetadatas) error {
	deploymentIDs := instances.DeploymentIDs()
	return h.foreverRetrier.Attempt(func() error {
		deploymentInstances, err := h.mgr.Query(deploymentIDs)
		if err != nil {
			return fmt.Errorf("error querying instances: %v", err)
		}

		var (
			wg   sync.WaitGroup
			safe int64
		)
		for i := range deploymentInstances {
			i := i
			wg.Add(1)
			h.workers.Go(func() {
				defer wg.Done()

				if !deploymentInstances[i].IsHealthy() || deploymentInstances[i].IsDeploying() {
					return
				}
				if err := h.client.IsHealthy(instances[i].APIEndpoint); err != nil {
					return
				}
				atomic.AddInt64(&safe, 1)
			})
		}
		wg.Wait()

		if safe != int64(len(instances)) {
			return fmt.Errorf("only %d out of %d instances are safe to deploy", safe, len(instances))
		}
		return nil
	})
}

func (h helper) validate(targets []deploymentTarget) error {
	return h.foreverRetrier.Attempt(func() error {
		var (
			wg    sync.WaitGroup
			errCh = make(chan error, 1)
		)
		for i := range targets {
			i := i
			wg.Add(1)
			h.workers.Go(func() {
				defer wg.Done()

				validator := targets[i].Validator
				if validator == nil {
					return
				}
				if err := validator(); err != nil {
					err = fmt.Errorf("validation error for instance %s: %v", targets[i].Instance.PlacementID, err)
					select {
					case errCh <- err:
					default:
					}
				}
			})
		}
		wg.Wait()
		close(errCh)
		return <-errCh
	})
}

func (h helper) resign(targets []deploymentTarget) error {
	return h.retrier.Attempt(func() error {
		var (
			wg    sync.WaitGroup
			errCh = make(chan error, 1)
		)
		for i := range targets {
			i := i
			wg.Add(1)
			h.workers.Go(func() {
				defer wg.Done()

				instance := targets[i].Instance
				if err := h.client.Resign(instance.APIEndpoint); err != nil {
					err = fmt.Errorf("resign error for instance %s: %v", instance.PlacementID, err)
					select {
					case errCh <- err:
					default:
					}
				}
			})
		}
		wg.Wait()
		close(errCh)
		return <-errCh
	})
}

func (h helper) deploy(targets []deploymentTarget, revision string) error {
	deploymentIDs := make([]string, 0, len(targets))
	for _, target := range targets {
		deploymentIDs = append(deploymentIDs, target.Instance.DeploymentID)
	}
	return h.retrier.Attempt(func() error {
		return h.mgr.Deploy(deploymentIDs, revision)
	})
}

func (h helper) waitUntilProgressing(
	instances instanceMetadatas,
	revision string,
) error {
	deploymentIDs := instances.DeploymentIDs()
	return h.foreverRetrier.Attempt(func() error {
		deploymentInstances, err := h.mgr.Query(deploymentIDs)
		if err != nil {
			return fmt.Errorf("error querying instances: %v", err)
		}

		for _, di := range deploymentInstances {
			if di.IsDeploying() || di.Revision() == revision {
				return nil
			}
		}

		return errNoDeploymentProgress
	})
}

func (h helper) allInstanceMetadatas() (instanceMetadatas, error) {
	placementInstances, err := h.placementInstances()
	if err != nil {
		return nil, fmt.Errorf("unable to determine instances from placement: %v", err)
	}
	deploymentInstances, err := h.mgr.QueryAll()
	if err != nil {
		return nil, fmt.Errorf("unable to query all instances from deployment: %v", err)
	}
	metadatas, err := h.computeInstanceMetadatas(placementInstances, deploymentInstances)
	if err != nil {
		return nil, fmt.Errorf("unable to compute instance metadatas: %v", err)
	}
	return metadatas, nil
}

// validateInstances validates instances derived from placement against
// instances derived from deployment, ensuring there are no duplicate instances
// and the instances derived from two sources match against each other.
func (h helper) computeInstanceMetadatas(
	placementInstances []services.PlacementInstance,
	deploymentInstances []Instance,
) (instanceMetadatas, error) {
	if len(placementInstances) != len(deploymentInstances) {
		errMsg := "number of instances is %d in the placement and %d in the deployment"
		return nil, fmt.Errorf(errMsg, len(placementInstances), len(deploymentInstances))
	}

	// Populate instance metadata from placement information.
	metadatas := make(instanceMetadatas, len(placementInstances))
	unique := make(map[string]int)
	for i, pi := range placementInstances {
		id := pi.ID()
		_, exists := unique[id]
		if exists {
			return nil, fmt.Errorf("instance %s not unique in the placement", id)
		}
		endpoint := pi.Endpoint()
		apiEndpoint, err := h.toAPIEndpointFn(endpoint)
		if err != nil {
			return nil, fmt.Errorf("unable to convert placement endpoint %s to api endpoint: %v", endpoint, err)
		}
		unique[id] = i
		metadatas[i].PlacementID = id
		metadatas[i].ShardSetID = pi.ShardSetID()
		metadatas[i].APIEndpoint = apiEndpoint
	}

	// Populate instance metadata from deployment information.
	for _, di := range deploymentInstances {
		id := di.ID()
		placementID, err := h.toPlacementIDFn(id)
		if err != nil {
			return nil, fmt.Errorf("unable to convert deployment instance id %s to placement instance id", id)
		}
		idx, exists := unique[placementID]
		if !exists {
			return nil, fmt.Errorf("instance %s is in deployment but not in placement", id)
		}
		if metadatas[idx].DeploymentID != "" {
			return nil, fmt.Errorf("instance %s not unique in the deployment", id)
		}
		metadatas[idx].DeploymentID = id
		metadatas[idx].Revision = di.Revision()
	}

	return metadatas, nil
}

func (h helper) placementInstances() ([]services.PlacementInstance, error) {
	stagedPlacement, onStagedPlacementDoneFn, err := h.placementWatcher.ActiveStagedPlacement()
	if err != nil {
		return nil, err
	}
	defer onStagedPlacementDoneFn()

	placement, onPlacementDoneFn, err := stagedPlacement.ActivePlacement()
	if err != nil {
		return nil, err
	}
	defer onPlacementDoneFn()

	return placement.Instances(), nil
}

func filterByRevision(metadatas instanceMetadatas, revision string) instanceMetadatas {
	filtered := make(instanceMetadatas, 0, len(metadatas))
	for _, metadata := range metadatas {
		if metadata.Revision == revision {
			continue
		}
		filtered = append(filtered, metadata)
	}
	return filtered
}

// instanceMetadata contains instance metadata.
type instanceMetadata struct {
	PlacementID  string
	ShardSetID   string
	APIEndpoint  string
	DeploymentID string
	Revision     string
}

type instanceMetadatas []instanceMetadata

func (m instanceMetadatas) DeploymentIDs() []string {
	res := make([]string, 0, len(m))
	for _, metadata := range m {
		res = append(res, metadata.DeploymentID)
	}
	return res
}
