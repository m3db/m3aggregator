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

	// ElectionStatus returns the election status.
	ElectionStatus() electionStatus

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

type electionManagerStatus int

const (
	electionManagerNotOpen electionManagerStatus = iota
	electionManagerOpen
	electionManagerResigning
	electionManagerResigned
	electionManagerClosed
)

type electionStatus int

const (
	followerStatus electionStatus = iota
	leaderStatus
)

type electionManagerMetrics struct {
	campaignCreateErrors tally.Counter
	campaignErrors       tally.Counter
	leaderErrors         tally.Counter
	leaderNotChanged     tally.Counter
	resignErrors         tally.Counter
	resignNotFollower    tally.Counter
	followerToLeader     tally.Counter
	leaderToFollower     tally.Counter
	electionStatus       tally.Gauge
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
		electionStatus:       scope.Gauge("election-status"),
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

	status           electionManagerStatus
	resignCh         chan struct{}
	doneCh           chan struct{}
	electionKey      string
	electionWg       sync.WaitGroup
	electionStatus   electionStatus
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
		status:          electionManagerNotOpen,
		resignCh:        make(chan struct{}, 1),
		doneCh:          make(chan struct{}),
		electionStatus:  followerStatus,
		sleepFn:         time.Sleep,
		metrics:         newElectionManagerMetrics(instrumentOpts.MetricsScope()),
	}
	go mgr.reportMetrics()
	return mgr
}

func (mgr *electionManager) Open(shardSetID string) error {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.status != electionManagerNotOpen {
		return errElectionManagerExpectNotOpen
	}
	mgr.status = electionManagerOpen
	mgr.electionStatus = followerStatus
	mgr.electionKey = fmt.Sprintf(mgr.electionKeyFmt, shardSetID)
	mgr.electionWg.Add(1)
	go mgr.startElectionLoop()
	return nil
}

func (mgr *electionManager) ElectionStatus() electionStatus {
	mgr.RLock()
	electionStatus := mgr.electionStatus
	mgr.RUnlock()
	return electionStatus
}

func (mgr *electionManager) Resign(ctx context.Context) error {
	mgr.Lock()
	if mgr.status != electionManagerOpen {
		mgr.Unlock()
		return errElectionManagerExpectOpen
	}
	mgr.status = electionManagerResigning
	mgr.Unlock()

	err := mgr.leaderService.Resign(mgr.electionKey)
	if err != nil {
		mgr.Lock()
		mgr.status = electionManagerOpen
		mgr.Unlock()
		err := fmt.Errorf("leader service resign failed: %v", err)
		mgr.metrics.resignErrors.Inc(1)
		mgr.logError("resign failed", err)
		return err
	}

	// Wait for the election loop to exit after resigning.
	mgr.resignCh <- struct{}{}
	mgr.electionWg.Wait()

	// Now wait for the current instance to become a follower.
	var (
		ctxDone           = false
		isFollower        = false
		statusCheckPeriod = time.Second
	)

	for {
		select {
		case <-ctx.Done():
			ctxDone = true
		default:
			isFollower = mgr.ElectionStatus() == followerStatus
		}
		if ctxDone || isFollower {
			break
		}
		mgr.sleepFn(statusCheckPeriod)
	}

	// If the context expires, we cancel the in progress change and re-check
	// the election status.
	if !isFollower {
		mgr.cancelInProgressChange()
		isFollower = mgr.ElectionStatus() == followerStatus
	}

	// If the election status is not follower, it means we failed to establish
	// the follower status with kv and as such we change the status back to
	// open and restart the election loop to restore the state before the resignation.
	if !isFollower {
		mgr.Lock()
		mgr.status = electionManagerOpen
		mgr.startElectionLoop()
		mgr.Unlock()
		err := fmt.Errorf("instance resigned but is not follower: %v", ctx.Err())
		mgr.metrics.resignNotFollower.Inc(1)
		mgr.logError("resign failed", err)
		return err
	}

	// We have successfully resigned.
	mgr.Lock()
	mgr.status = electionManagerResigned
	mgr.Unlock()
	return nil
}

func (mgr *electionManager) Close() error {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.status != electionManagerOpen && mgr.status != electionManagerResigned {
		return errElectionManagerExpectOpenOrResigned
	}
	mgr.status = electionManagerClosed
	close(mgr.resignCh)
	close(mgr.doneCh)
	return nil
}

func (mgr *electionManager) startElectionLoop() {
	defer mgr.electionWg.Done()

	var campaignStatusCh <-chan campaign.Status
	notResigned := func(int) bool { return !mgr.isResigned() }

	for {
		if campaignStatusCh == nil {
			// NB(xichen): campaign retrier retries forever until either the Campaign call succeeds,
			// or a signal is received on the doneCh channel.
			mgr.campaignRetrier.AttemptWhile(notResigned, func() error {
				var err error
				campaignStatusCh, err = mgr.leaderService.Campaign(mgr.electionKey, mgr.campaignOpts)
				if err == nil {
					return nil
				}
				mgr.metrics.campaignCreateErrors.Inc(1)
				mgr.logError("error creating campaign", err)
				return err
			})
		}

		if mgr.isResigned() {
			return
		}

		var (
			campaignStatus campaign.Status
			ok             bool
		)
		select {
		case campaignStatus, ok = <-campaignStatusCh:
			// If the campaign status channel is closed (e.g., because session has expired, or
			// we have resigned from the campaign, or there are issues with the underlying etcd
			// cluster), we restart the campaign.
			if !ok {
				campaignStatusCh = nil
				continue
			}
		case <-mgr.resignCh:
			return
		}

		if campaignStatus.State == campaign.Error {
			mgr.metrics.campaignErrors.Inc(1)
			mgr.logError("error campaigning", campaignStatus.Err)
			continue
		}

		currStatus := mgr.ElectionStatus()
		newStatus := followerStatus
		if campaignStatus.State == campaign.Leader {
			newStatus = leaderStatus
		}
		mgr.changeElectionStatus(currStatus, newStatus)
	}
}

func (mgr *electionManager) changeElectionStatus(currStatus, newStatus electionStatus) {
	if currStatus == newStatus {
		if newStatus != leaderStatus {
			return
		}
		// We are in the middle of verifying leader and changing from leader to follower
		// and we received an update that indicates the current instance is now the leader,
		// therefore we cancel the change from leader to follower and instead set the
		// election status to leader.
		if mgr.cancelInProgressChange() {
			// NB(xichen): if an in-progress change was cancelled, we need to explicitly set
			// election status to leader in case the role changing goroutine managed to change
			// the status to follower just before it was cancelled.
			mgr.Lock()
			mgr.electionStatus = leaderStatus
			mgr.Unlock()
		}
		return
	}

	// Changing role from follower to leader.
	if currStatus == followerStatus && newStatus == leaderStatus {
		mgr.Lock()
		mgr.electionStatus = newStatus
		mgr.Unlock()
		mgr.metrics.followerToLeader.Inc(1)
		mgr.logger.Info("election status changed from follower to leader")
		return
	}

	// NB(xichen): in case of a leader to follower transition, we need to be extra
	// careful and should confirm the leader role has been actually claimed before
	// setting the election status to follower. Otherwise, we might get into a situation
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

// cancelInProgressChange cancels in progress election status changes if any,
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
		mgr.electionStatus = followerStatus
		mgr.changeCancelFn = nil
		mgr.changeInProgress = false
		mgr.Unlock()
		mgr.metrics.leaderToFollower.Inc(1)
		mgr.logger.Info("election status changed from leader to follower")
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
			currStatus := mgr.ElectionStatus()
			mgr.metrics.electionStatus.Update(float64(currStatus))
		case <-mgr.doneCh:
			ticker.Stop()
			return
		}
	}
}

func (mgr *electionManager) logError(desc string, err error) {
	mgr.logger.WithFields(
		xlog.NewLogField("electionKey", mgr.electionKey),
		xlog.NewLogField("electionTTL", time.Duration(mgr.electionOpts.TTL())*time.Second),
		xlog.NewLogField("leaderValue", mgr.campaignOpts.LeaderValue()),
		xlog.NewLogErrField(err),
	).Error(desc)
}
