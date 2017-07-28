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

package aggregator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/leader/campaign"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/retry"

	"github.com/uber-go/tally"
)

// ElectionManager manages leadership elections.
type ElectionManager interface {
	// Open opens the election manager for a given shard set.
	Open(shardSetID string) error

	// ElectionState returns the election state.
	ElectionState() ElectionState

	// Resign stops the election and resigns from the ongoing campaign if any, thereby
	// forcing the current instance to become a follower. If the provided context
	// expires before resignation is complete, the context error is returned, and the
	// election is restarted if necessary.
	Resign(ctx context.Context) error

	// Close the election manager.
	Close() error
}

var (
	errElectionManagerExpectNotOpen        = errors.New("election manager is expected to be not open")
	errElectionManagerExpectOpen           = errors.New("election manager is expected to be open")
	errElectionManagerExpectOpenOrResigned = errors.New("election manager is expected to be open or resigned")
	errLeaderNotChanged                    = errors.New("leader has not changed")
)

type electionManagerState int

const (
	electionManagerNotOpen electionManagerState = iota
	electionManagerOpen
	electionManagerResigning
	electionManagerResigned
	electionManagerClosed
)

// ElectionState is the election state.
type ElectionState int

// A list of supported election states.
const (
	FollowerState ElectionState = iota
	LeaderState
)

func (state ElectionState) String() string {
	switch state {
	case FollowerState:
		return "follower"
	case LeaderState:
		return "leader"
	default:
		panic(fmt.Sprintf("unknown election state %v", int(state)))
	}
}

type electionManagerMetrics struct {
	campaignCreateErrors tally.Counter
	campaignErrors       tally.Counter
	leaderErrors         tally.Counter
	leaderNotChanged     tally.Counter
	resignErrors         tally.Counter
	resignNotFollower    tally.Counter
	followerToLeader     tally.Counter
	leaderToFollower     tally.Counter
	electionState        tally.Gauge
}

func newElectionManagerMetrics(scope tally.Scope) electionManagerMetrics {
	return electionManagerMetrics{
		campaignCreateErrors: scope.Counter("campaign-create-errors"),
		campaignErrors:       scope.Counter("campaign-errors"),
		leaderErrors:         scope.Counter("leader-errors"),
		leaderNotChanged:     scope.Counter("leader-not-changed"),
		resignErrors:         scope.Counter("resign-errors"),
		resignNotFollower:    scope.Counter("resign-not-follower"),
		followerToLeader:     scope.Counter("follower-to-leader"),
		leaderToFollower:     scope.Counter("leader-to-follower"),
		electionState:        scope.Gauge("election-state"),
	}
}

type electionManager struct {
	sync.RWMutex

	nowFn           clock.NowFn
	logger          xlog.Logger
	reportInterval  time.Duration
	campaignOpts    services.CampaignOptions
	electionOpts    services.ElectionOptions
	campaignRetrier xretry.Retrier
	changeRetrier   xretry.Retrier
	electionKeyFmt  string
	leaderService   services.LeaderService
	leaderValue     string

	state            electionManagerState
	doneCh           chan struct{}
	resignCh         chan struct{}
	resignWg         *sync.WaitGroup
	electionKey      string
	electionWg       sync.WaitGroup
	electionState    ElectionState
	changeInProgress bool
	changeCancelFn   context.CancelFunc
	changeWg         sync.WaitGroup
	sleepFn          sleepFn
	metrics          electionManagerMetrics
}

// NewElectionManager creates a new election manager.
func NewElectionManager(opts ElectionManagerOptions) ElectionManager {
	instrumentOpts := opts.InstrumentOptions()
	campaignOpts := opts.CampaignOptions()
	campaignRetrier := xretry.NewRetrier(opts.CampaignRetryOptions().SetForever(true))
	changeRetrier := xretry.NewRetrier(opts.ChangeRetryOptions().SetForever(true))
	mgr := &electionManager{
		nowFn:           opts.ClockOptions().NowFn(),
		logger:          instrumentOpts.Logger(),
		reportInterval:  instrumentOpts.ReportInterval(),
		campaignOpts:    campaignOpts,
		electionOpts:    opts.ElectionOptions(),
		campaignRetrier: campaignRetrier,
		changeRetrier:   changeRetrier,
		electionKeyFmt:  opts.ElectionKeyFmt(),
		leaderService:   opts.LeaderService(),
		leaderValue:     campaignOpts.LeaderValue(),
		state:           electionManagerNotOpen,
		resignCh:        make(chan struct{}),
		doneCh:          make(chan struct{}),
		electionState:   FollowerState,
		sleepFn:         time.Sleep,
		metrics:         newElectionManagerMetrics(instrumentOpts.MetricsScope()),
	}
	go mgr.reportMetrics()
	return mgr
}

func (mgr *electionManager) Open(shardSetID string) error {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.state != electionManagerNotOpen {
		return errElectionManagerExpectNotOpen
	}
	mgr.state = electionManagerOpen
	mgr.electionKey = fmt.Sprintf(mgr.electionKeyFmt, shardSetID)
	mgr.electionWg.Add(1)
	go mgr.startElectionLoop()
	return nil
}

func (mgr *electionManager) ElectionState() ElectionState {
	mgr.RLock()
	electionState := mgr.electionState
	mgr.RUnlock()
	return electionState
}

func (mgr *electionManager) Resign(ctx context.Context) error {
	mgr.Lock()
	if mgr.state != electionManagerOpen {
		mgr.Unlock()
		return errElectionManagerExpectOpen
	}
	mgr.state = electionManagerResigning
	mgr.resignWg = &sync.WaitGroup{}
	resignWg := mgr.resignWg
	resignWg.Add(1)
	mgr.Unlock()

	err := mgr.leaderService.Resign(mgr.electionKey)
	if err != nil {
		resignWg.Done()
		mgr.Lock()
		mgr.state = electionManagerOpen
		mgr.resignWg = nil
		mgr.Unlock()
		err := fmt.Errorf("leader service resign failed: %v", err)
		mgr.metrics.resignErrors.Inc(1)
		mgr.logError("resign failed", err)
		return err
	}

	// Wait for the election loop to exit after resigning.
	close(mgr.resignCh)
	resignWg.Done()
	mgr.electionWg.Wait()

	// Now wait for the current instance to become a follower.
	var (
		ctxDone          = false
		isFollower       = false
		stateCheckPeriod = time.Second
	)

	for {
		select {
		case <-ctx.Done():
			ctxDone = true
		default:
			isFollower = mgr.ElectionState() == FollowerState
		}
		if ctxDone || isFollower {
			break
		}
		mgr.sleepFn(stateCheckPeriod)
	}

	// If the context expires, we cancel the in progress change and re-check
	// the election state.
	if !isFollower {
		mgr.cancelInProgressChange()
		isFollower = mgr.ElectionState() == FollowerState
	}

	// If the election state is not follower, it means we failed to establish
	// the follower state with kv and as such we change the election manager state
	// back to open and restart the election loop to restore the state before the resignation.
	if !isFollower {
		mgr.Lock()
		mgr.state = electionManagerOpen
		mgr.resignCh = make(chan struct{})
		mgr.resignWg = nil
		mgr.electionWg.Add(1)
		go mgr.startElectionLoop()
		mgr.Unlock()
		err := fmt.Errorf("instance resigned but is not follower: %v", ctx.Err())
		mgr.metrics.resignNotFollower.Inc(1)
		mgr.logError("resign failed", err)
		return err
	}

	// We have successfully resigned.
	mgr.Lock()
	mgr.state = electionManagerResigned
	mgr.resignWg = nil
	mgr.Unlock()
	return nil
}

func (mgr *electionManager) Close() error {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.state != electionManagerOpen && mgr.state != electionManagerResigned {
		return errElectionManagerExpectOpenOrResigned
	}
	if mgr.state == electionManagerOpen {
		close(mgr.resignCh)
	}
	close(mgr.doneCh)
	mgr.state = electionManagerClosed
	return nil
}

func (mgr *electionManager) startElectionLoop() {
	defer mgr.electionWg.Done()

	var campaignStatusCh <-chan campaign.Status
	notResigned := func(int) bool { return !mgr.isResigned() }

	for {
		if campaignStatusCh == nil {
			mgr.RLock()
			state := mgr.state
			resignWg := mgr.resignWg
			mgr.RUnlock()

			// NB(xichen): need to wait for the result of the leader service resign call
			// because resigning is done in a different goroutine and whether we need to
			// restart the campaign depends on the result of the resign call.
			if state == electionManagerResigning {
				resignWg.Wait()
			}

			// NB(xichen): campaign retrier retries forever until either the Campaign call succeeds,
			// or the election manager has resigned.
			if err := mgr.campaignRetrier.AttemptWhile(notResigned, func() error {
				var err error
				campaignStatusCh, err = mgr.leaderService.Campaign(mgr.electionKey, mgr.campaignOpts)
				if err == nil {
					return nil
				}
				mgr.metrics.campaignCreateErrors.Inc(1)
				mgr.logError("error creating campaign", err)
				return err
			}); err == xretry.ErrWhileConditionFalse {
				return
			}
		}

		// NB(xichen): we don't return on resignCh close because we would like to process all
		// status updates on the campaign status channel before returning or otherwise we might
		// miss status updates after resigning from the underlying etcd campaign.
		var (
			campaignStatus campaign.Status
			ok             bool
		)
		select {
		case campaignStatus, ok = <-campaignStatusCh:
			// If the campaign status channel is closed (e.g., because session has expired, or
			// we have resigned from the campaign, or there are issues with the underlying etcd
			// cluster), we restart the campaign loop.
			if !ok {
				campaignStatusCh = nil
				continue
			}
		case <-mgr.doneCh:
			return
		}
		mgr.processStatusUpdate(campaignStatus)
	}
}

func (mgr *electionManager) processStatusUpdate(campaignStatus campaign.Status) {
	if campaignStatus.State == campaign.Error {
		mgr.metrics.campaignErrors.Inc(1)
		mgr.logError("error campaigning", campaignStatus.Err)
		return
	}

	currState := mgr.ElectionState()
	newState := FollowerState
	if campaignStatus.State == campaign.Leader {
		newState = LeaderState
	}
	mgr.changeElectionState(currState, newState)
}

func (mgr *electionManager) changeElectionState(currState, newState ElectionState) {
	if currState == newState {
		if newState != LeaderState {
			return
		}
		// We are in the middle of verifying leader and changing from leader to follower
		// and we received an update that indicates the current instance is now the leader,
		// therefore we cancel the change from leader to follower and instead set the
		// election state to leader.
		if mgr.cancelInProgressChange() {
			// NB(xichen): if an in-progress change was cancelled, we need to explicitly set
			// election state to leader in case the role changing goroutine managed to change
			// the state to follower just before it was cancelled.
			mgr.Lock()
			mgr.electionState = LeaderState
			mgr.Unlock()
		}
		return
	}

	// Changing role from follower to leader.
	if currState == FollowerState && newState == LeaderState {
		mgr.Lock()
		mgr.electionState = newState
		mgr.Unlock()
		mgr.metrics.followerToLeader.Inc(1)
		mgr.logger.Info("election state changed from follower to leader")
		return
	}

	// NB(xichen): in case of a leader to follower transition, we need to be extra
	// careful and should confirm the leader role has been actually claimed before
	// setting the election state to follower. Otherwise, we might get into a situation
	// where we resigned from the leader role but the other side (i.e., the old follower)
	// is not claiming the leader role due to e.g., hardware/network issues and as such,
	// neither instances will be flushing data downstream.
	mgr.Lock()
	defer mgr.Unlock()
	if mgr.changeInProgress {
		return
	}
	mgr.changeInProgress = true
	ctx, cancelFn := context.WithCancel(context.Background())
	mgr.changeCancelFn = cancelFn
	mgr.changeWg.Add(1)
	go mgr.leaderToFollower(ctx)
}

// cancelInProgressChange cancels in progress election state changes if any,
// returning true if a change is cancelled and false otherwise.
func (mgr *electionManager) cancelInProgressChange() bool {
	mgr.Lock()
	if !mgr.changeInProgress {
		mgr.Unlock()
		return false
	}
	cancelFn := mgr.changeCancelFn
	mgr.changeCancelFn = nil
	mgr.changeInProgress = false
	mgr.Unlock()
	cancelFn()
	mgr.changeWg.Wait()
	return true
}

func (mgr *electionManager) leaderToFollower(ctx context.Context) {
	defer mgr.changeWg.Done()

	continueFn := func(int) bool {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}

	// NB(xichen): majority of the verification logic below will be moved into the
	// leadership API implementation to ensure the API user will receive a follower
	// status only after this verification is performed.
	mgr.changeRetrier.AttemptWhile(continueFn, func() error {
		leader, err := mgr.leaderService.Leader(mgr.electionKey)
		if err != nil {
			mgr.metrics.leaderErrors.Inc(1)
			mgr.logError("error determining the leader", err)
			return err
		}
		if leader == mgr.leaderValue {
			mgr.metrics.leaderNotChanged.Inc(1)
			mgr.logError("leader has not changed", errLeaderNotChanged)
			return errLeaderNotChanged
		}
		mgr.Lock()
		mgr.electionState = FollowerState
		mgr.changeCancelFn = nil
		mgr.changeInProgress = false
		mgr.Unlock()
		mgr.metrics.leaderToFollower.Inc(1)
		mgr.logger.Info("election state changed from leader to follower")
		return nil
	})
}

func (mgr *electionManager) isResigned() bool {
	select {
	case <-mgr.resignCh:
		return true
	default:
		return false
	}
}

func (mgr *electionManager) reportMetrics() {
	ticker := time.NewTicker(mgr.reportInterval)
	for {
		select {
		case <-ticker.C:
			currState := mgr.ElectionState()
			mgr.metrics.electionState.Update(float64(currState))
		case <-mgr.doneCh:
			ticker.Stop()
			return
		}
	}
}

func (mgr *electionManager) logError(desc string, err error) {
	mgr.logger.WithFields(
		xlog.NewLogField("electionKey", mgr.electionKey),
		xlog.NewLogField("electionTTL", time.Duration(mgr.electionOpts.TTLSecs())*time.Second),
		xlog.NewLogField("leaderValue", mgr.campaignOpts.LeaderValue()),
		xlog.NewLogErrField(err),
	).Error(desc)
}
