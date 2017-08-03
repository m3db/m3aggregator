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
	"fmt"
	"sync"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3x/sync"
)

var (
	emptyPlan Plan
)

// Target is a deployment target.
type Target struct {
	Instance  services.PlacementInstance
	Validator Validator
}

func (t Target) String() string { return t.Instance.ID() }

// Step is a deployment step.
type Step struct {
	Targets []Target
}

// Plan is a deployment plan.
type Plan struct {
	Steps []Step
}

// Planner generates deployment plans for given instances under constraints.
type Planner interface {
	// GeneratePlan generates a deployment plan for given target instances.
	GeneratePlan(toDeploy, all []services.PlacementInstance) (Plan, error)
}

type planner struct {
	leaderService    services.LeaderService
	workers          xsync.WorkerPool
	electionKeyFmt   string
	maxStepSize      int
	validatorFactory validatorFactory
}

// NewPlanner creates a new deployment planner.
func NewPlanner(opts PlannerOptions) Planner {
	client := opts.AggregatorClient()
	workers := opts.WorkerPool()
	validatorFactory := newValidatorFactory(client, workers)
	return planner{
		leaderService:    opts.LeaderService(),
		workers:          opts.WorkerPool(),
		electionKeyFmt:   opts.ElectionKeyFmt(),
		maxStepSize:      opts.MaxStepSize(),
		validatorFactory: validatorFactory,
	}
}

func (p planner) GeneratePlan(
	toDeploy, all []services.PlacementInstance,
) (Plan, error) {
	grouped, err := p.groupInstancesByShardSetID(toDeploy, all)
	if err != nil {
		return emptyPlan, fmt.Errorf("unable to group instances by shard set id: %v", err)
	}
	return p.generatePlan(grouped, len(toDeploy), p.maxStepSize), nil
}

func (p planner) generatePlan(
	instances map[string]*instanceGroup,
	numInstances int,
	maxStepSize int,
) Plan {
	var (
		step  Step
		plan  Plan
		total = numInstances
	)
	for total > 0 {
		step = p.generateStep(instances, maxStepSize)
		plan.Steps = append(plan.Steps, step)
		total -= len(step.Targets)
	}
	return plan
}

func (p planner) generateStep(
	instances map[string]*instanceGroup,
	maxStepSize int,
) Step {
	// NB(xichen): we always choose instances that are currently in the follower state first,
	// unless there are no more follower instances, in which case we'll deploy the leader instances.
	// This is to reduce the overall deployment time due to reduced number of leader promotions and
	// as such we are less likely to need to wait for the follower instances to be ready to take over
	// the leader role.
	step := p.generateStepFromTargetType(instances, maxStepSize, followerTarget)

	// If we have found some follower instances to deploy, we don't attempt to deploy leader
	// instances in the same step even if we have not reached the max step size to avoid delaying
	// deploying to the followers due to deploying leader instances.
	if len(step.Targets) > 0 {
		return step
	}

	// If we have not found any followers, we proceed to deploy leader instances.
	return p.generateStepFromTargetType(instances, maxStepSize, leaderTarget)
}

func (p planner) generateStepFromTargetType(
	instances map[string]*instanceGroup,
	maxStepSize int,
	targetType targetType,
) Step {
	step := Step{Targets: make([]Target, 0, maxStepSize)}
	for shardSetID, group := range instances {
		if len(group.ToDeploy) == 0 {
			delete(instances, shardSetID)
			continue
		}
		for i, instance := range group.ToDeploy {
			if !matchTargetType(instance.ID(), group.LeaderID, targetType) {
				continue
			}
			target := Target{
				Instance:  instance,
				Validator: p.validatorFactory.ValidatorFor(instance, group, targetType),
			}
			step.Targets = append(step.Targets, target)
			group.removeInstanceToDeploy(i)
			if maxStepSize != 0 && len(step.Targets) >= maxStepSize {
				return step
			}
			break
		}
	}
	return step
}

func (p planner) groupInstancesByShardSetID(
	toDeploy, all []services.PlacementInstance,
) (map[string]*instanceGroup, error) {
	grouped := make(map[string]*instanceGroup, len(toDeploy))

	// Group the instances to be deployed by shard set id.
	for _, instance := range toDeploy {
		shardSetID := instance.ShardSetID()
		group, exists := grouped[shardSetID]
		if !exists {
			group = &instanceGroup{
				ToDeploy: make([]services.PlacementInstance, 0, 2),
				All:      make([]services.PlacementInstance, 0, 2),
			}
		}
		group.ToDeploy = append(group.ToDeploy, instance)
		grouped[shardSetID] = group
	}

	// Determine the full set of instances in each group.
	for _, instance := range all {
		shardSetID := instance.ShardSetID()
		group, exists := grouped[shardSetID]
		if !exists {
			continue
		}
		group.All = append(group.All, instance)
	}

	// Determine the leader of each group.
	var (
		wg    sync.WaitGroup
		errCh = make(chan error, 1)
	)
	for shardSetID, group := range grouped {
		shardSetID, group := shardSetID, group
		wg.Add(1)
		p.workers.Go(func() {
			defer wg.Done()

			electionKey := fmt.Sprintf(p.electionKeyFmt, shardSetID)
			leader, err := p.leaderService.Leader(electionKey)
			if err != nil {
				err = fmt.Errorf("unable to determine leader for shard set id %s", shardSetID)
				select {
				case errCh <- err:
				default:
				}
				return
			}
			for _, instance := range group.All {
				if instance.ID() == leader {
					group.LeaderID = instance.ID()
					return
				}
			}
			err = fmt.Errorf("unknown leader %s for shard set id %s", leader, shardSetID)
			select {
			case errCh <- err:
			default:
			}
		})
	}

	wg.Wait()
	close(errCh)
	if err := <-errCh; err != nil {
		return nil, err
	}
	return grouped, nil
}

type targetType int

const (
	followerTarget targetType = iota
	leaderTarget
)

func matchTargetType(
	instanceID string,
	leaderID string,
	targetType targetType,
) bool {
	if targetType == leaderTarget {
		return instanceID == leaderID
	}
	return instanceID != leaderID
}

type instanceGroup struct {
	// LeaderID is the instance id of the leader in the group.
	LeaderID string

	// ToDeploy are the instances to be deployed in the group.
	ToDeploy []services.PlacementInstance

	// All include all the instances in the group regardless of whether they need to be deployed.
	All []services.PlacementInstance
}

func (group *instanceGroup) removeInstanceToDeploy(i int) {
	lastIdx := len(group.ToDeploy) - 1
	group.ToDeploy[i], group.ToDeploy[lastIdx] = group.ToDeploy[lastIdx], group.ToDeploy[i]
	group.ToDeploy = group.ToDeploy[:lastIdx]
}
