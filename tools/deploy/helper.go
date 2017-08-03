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

// Helper is a helper class handling deployments.
type Helper interface {
	// Deploy deploys a target revision to the instances in the placement.
	Deploy(revision string) error
}

// TODO(xichen): disable deployment while another is ongoing.
type helper struct {
	logger             xlog.Logger
	mgr                Manager
	planner            Planner
	client             AggregatorClient
	store              kv.Store
	retrier            xretry.Retrier
	workers            xsync.WorkerPool
	placementWatcher   services.StagedPlacementWatcher
	settleBetweenSteps time.Duration
}

// NewHelper creates a new deployment helper.
func NewHelper(opts HelperOptions) (Helper, error) {
	placementWatcherOpts := opts.StagedPlacementWatcherOptions()
	placementWatcher := placement.NewStagedPlacementWatcher(placementWatcherOpts)
	if err := placementWatcher.Watch(); err != nil {
		return nil, err
	}
	return helper{
		logger:             opts.InstrumentOptions().Logger(),
		mgr:                opts.Manager(),
		planner:            opts.Planner(),
		client:             opts.AggregatorClient(),
		store:              opts.KVStore(),
		retrier:            opts.Retrier(),
		workers:            opts.WorkerPool(),
		placementWatcher:   placementWatcher,
		settleBetweenSteps: opts.SettleDurationBetweenSteps(),
	}, nil
}

func (h helper) Deploy(revision string) error {
	if revision == "" {
		return errInvalidRevision
	}
	placementInstances, err := h.placementInstances()
	if err != nil {
		return fmt.Errorf("unable to determine instances from placement: %v", err)
	}

	deploymentInstances, err := h.mgr.Query(placementInstances)
	if err != nil {
		return fmt.Errorf("unable to query instances from deployment: %v", err)
	}

	if err := validateInstances(placementInstances, deploymentInstances); err != nil {
		return fmt.Errorf("unable to validate instances: %v", err)
	}

	filtered := filterInstancesByRevision(placementInstances, deploymentInstances, revision)
	deploymentPlan, err := h.planner.GeneratePlan(filtered, placementInstances)
	if err != nil {
		return fmt.Errorf("unable to generate deployment plan: %v", err)
	}

	if err = h.execute(deploymentPlan, revision, filtered, placementInstances); err != nil {
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
	plan Plan,
	revision string,
	toDeploy, allInstances []services.PlacementInstance,
) error {
	numSteps := len(plan.Steps)
	for i, step := range plan.Steps {
		h.logger.Infof("deploying step %d of %d", i+1, numSteps)
		if err := h.executeStep(step, revision, toDeploy, allInstances); err != nil {
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
	step Step,
	revision string,
	toDeploy, allInstances []services.PlacementInstance,
) error {
	h.logger.Infof("waiting until safe to deploy for step %v", step)
	if err := h.waitUntilSafe(allInstances); err != nil {
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
	if err := h.waitUntilSafe(allInstances); err != nil {
		return err
	}
	return nil
}

func (h helper) waitUntilSafe(instances []services.PlacementInstance) error {
	return h.retrier.Attempt(func() error {
		deploymentInstances, err := h.mgr.Query(instances)
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
				if err := h.client.IsHealthy(instances[i].Endpoint()); err != nil {
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

func (h helper) validate(targets []Target) error {
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

				validator := targets[i].Validator
				if validator == nil {
					return
				}
				if err := validator(); err != nil {
					err = fmt.Errorf("validation error for instance %s: %v", targets[i].Instance.ID(), err)
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

func (h helper) resign(targets []Target) error {
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
				if err := h.client.Resign(instance.Endpoint()); err != nil {
					err = fmt.Errorf("resign error for instance %s: %v", instance.ID(), err)
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

func (h helper) deploy(targets []Target, revision string) error {
	instances := make([]services.PlacementInstance, 0, len(targets))
	for _, target := range targets {
		instances = append(instances, target.Instance)
	}
	return h.retrier.Attempt(func() error {
		return h.mgr.Deploy(instances, revision)
	})
}

func (h helper) waitUntilProgressing(
	instances []services.PlacementInstance,
	revision string,
) error {
	return h.retrier.Attempt(func() error {
		deploymentInstances, err := h.mgr.Query(instances)
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

func (h helper) placementInstances() ([]services.PlacementInstance, error) {
	placement, err := h.placement()
	if err != nil {
		return nil, err
	}
	return placement.Instances(), nil
}

func (h helper) placement() (services.Placement, error) {
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

	return placement, nil
}

func filterInstancesByRevision(
	placementInstances []services.PlacementInstance,
	deploymentInstances []Instance,
	revision string,
) []services.PlacementInstance {
	filtered := make([]services.PlacementInstance, 0, len(placementInstances))
	for i, di := range deploymentInstances {
		if di.Revision() == revision {
			continue
		}
		filtered = append(filtered, placementInstances[i])
	}
	return filtered
}

// validateInstances validates instances derived from placement against
// instances derived from deployment, ensuring there are no duplicate instances
// and the instances derived from two sources match against each other.
func validateInstances(
	placementInstances []services.PlacementInstance,
	deploymentInstances []Instance,
) error {
	if len(placementInstances) != len(deploymentInstances) {
		errMsg := "number of instances is %d in the placement and %d in the deployment"
		return fmt.Errorf(errMsg, len(placementInstances), len(deploymentInstances))
	}
	unique := make(map[string]int)
	for _, pi := range placementInstances {
		id := pi.ID()
		_, exists := unique[id]
		if exists {
			return fmt.Errorf("instance %s not unique in the placement", id)
		}
		unique[id] = 1
	}
	for _, di := range deploymentInstances {
		id := di.ID()
		v, exists := unique[id]
		if !exists {
			return fmt.Errorf("instance %s is in deployment but not in placement", id)
		}
		v--
		if v < 0 {
			return fmt.Errorf("instance %s not unique in the deployment", id)
		}
		unique[id] = v
	}
	return nil
}
