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
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/leader/campaign"

	"github.com/stretchr/testify/require"
)

func TestElectionManagerOpenAlreadyOpen(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.state = electionManagerOpen
	require.Equal(t, errElectionManagerExpectNotOpen, mgr.Open(testShardSetID))
}

func TestElectionManagerOpenSuccess(t *testing.T) {
	leaderService := &mockLeaderService{
		campaignFn: func(
			electionID string,
			opts services.CampaignOptions,
		) (<-chan campaign.Status, error) {
			return make(chan campaign.Status), nil
		},
	}
	opts := testElectionManagerOptions(t).SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)
	require.NoError(t, mgr.Open(testShardSetID))
	require.NoError(t, mgr.Close())
}

func TestElectionManagerElectionState(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.Lock()
	mgr.electionState = LeaderState
	mgr.Unlock()
	require.Equal(t, LeaderState, mgr.ElectionState())
}

func TestElectionManagerResignNotOpen(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	require.Equal(t, errElectionManagerExpectOpen, mgr.Resign(context.Background()))
}

func TestElectionManagerResignLeaderServiceResignError(t *testing.T) {
	errLeaderServiceResign := errors.New("leader service resign error")
	leaderService := &mockLeaderService{
		campaignFn: func(
			electionID string,
			opts services.CampaignOptions,
		) (<-chan campaign.Status, error) {
			return make(chan campaign.Status), nil
		},
		resignFn: func(electionID string) error {
			return errLeaderServiceResign
		},
	}
	opts := testElectionManagerOptions(t).SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)
	require.NoError(t, mgr.Open(testShardSetID))
	require.Error(t, mgr.Resign(context.Background()))
	require.Equal(t, electionManagerOpen, mgr.state)
	require.NoError(t, mgr.Close())
}

func TestElectionManagerResignTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()

	leaderService := &mockLeaderService{
		campaignFn: func(
			electionID string,
			opts services.CampaignOptions,
		) (<-chan campaign.Status, error) {
			return make(chan campaign.Status), nil
		},
		resignFn: func(electionID string) error {
			return nil
		},
	}
	opts := testElectionManagerOptions(t).SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.Lock()
	mgr.electionState = LeaderState
	mgr.Unlock()
	require.NoError(t, mgr.Open(testShardSetID))
	require.Error(t, mgr.Resign(ctx))
	require.Equal(t, electionManagerOpen, mgr.state)
	require.NoError(t, mgr.Close())
}

func TestElectionManagerResignSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	leaderService := &mockLeaderService{
		campaignFn: func(
			electionID string,
			opts services.CampaignOptions,
		) (<-chan campaign.Status, error) {
			return make(chan campaign.Status), nil
		},
		resignFn: func(electionID string) error {
			return nil
		},
	}
	opts := testElectionManagerOptions(t).SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.Lock()
	mgr.electionState = LeaderState
	mgr.Unlock()
	require.NoError(t, mgr.Open(testShardSetID))

	// Update election state asynchronously.
	var updated bool
	go func() {
		time.Sleep(500 * time.Millisecond)
		mgr.Lock()
		updated = true
		mgr.electionState = FollowerState
		mgr.Unlock()
	}()

	require.NoError(t, mgr.Resign(ctx))
	require.Equal(t, electionManagerResigned, mgr.state)
	require.True(t, updated)
	require.NoError(t, mgr.Close())
}

func TestElectionManagerCloseNotOpenOrResigned(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.state = electionManagerNotOpen
	require.Equal(t, errElectionManagerExpectOpenOrResigned, mgr.Close())
}

func TestElectionManagerCloseSuccess(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.state = electionManagerOpen
	require.NoError(t, mgr.Close())
}

func TestElectionManagerStartElectionLoop(t *testing.T) {
	leaderValue := "myself"
	campaignCh := make(chan campaign.Status)
	leaderService := &mockLeaderService{
		campaignFn: func(
			electionID string,
			opts services.CampaignOptions,
		) (<-chan campaign.Status, error) {
			return campaignCh, nil
		},
		leaderFn: func(electionID string) (string, error) {
			return "someone else", nil
		},
	}
	campaignOpts, err := services.NewCampaignOptions()
	require.NoError(t, err)
	campaignOpts = campaignOpts.SetLeaderValue(leaderValue)
	opts := NewElectionManagerOptions().
		SetCampaignOptions(campaignOpts).
		SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)
	require.NoError(t, mgr.Open(testShardSetID))

	// Error status is ignored.
	campaignCh <- campaign.NewErrorStatus(errors.New("foo"))
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, FollowerState, mgr.ElectionState())

	// Same state is a no op.
	campaignCh <- campaign.NewStatus(campaign.Follower)
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, FollowerState, mgr.ElectionState())

	// Follower to leader.
	campaignCh <- campaign.NewStatus(campaign.Leader)
	for {
		if mgr.ElectionState() == LeaderState {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Leader to follower
	campaignCh <- campaign.NewStatus(campaign.Follower)
	for {
		if mgr.ElectionState() == FollowerState {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Asynchronously resign.
	go func() {
		mgr.state = electionManagerResigned
		campaignCh <- campaign.NewStatus(campaign.Leader)

		// Create a bit of lag between when campaign channel is written
		// and when the resign channel is closed.
		time.Sleep(100 * time.Millisecond)
		close(mgr.resignCh)
	}()
	for {
		if mgr.ElectionState() == LeaderState {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	require.NoError(t, mgr.Close())
}

func TestElectionManagerChangeElectionStateSameStatusNotLeader(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.changeElectionState(FollowerState, FollowerState)
	require.Equal(t, mgr.electionState, FollowerState)
}

func TestElectionManagerChangeElectionStateSameStatusLeaderWithInProgressChange(t *testing.T) {
	var cancelled int
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.changeInProgress = true
	mgr.changeCancelFn = func() { cancelled++ }
	mgr.changeElectionState(LeaderState, LeaderState)
	require.Equal(t, mgr.electionState, LeaderState)
	require.False(t, mgr.changeInProgress)
	require.Nil(t, mgr.changeCancelFn)
}

func TestElectionManagerChangeElectionStateFollowerToLeader(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.changeElectionState(FollowerState, LeaderState)
	require.Equal(t, mgr.electionState, LeaderState)
}

func TestElectionManagerChangeElectionStateLeaderToFollower(t *testing.T) {
	leaderValue := "myself"
	leaderService := &mockLeaderService{
		leaderFn: func(electionID string) (string, error) {
			return "someone else", nil
		},
	}
	campaignOpts, err := services.NewCampaignOptions()
	require.NoError(t, err)
	campaignOpts = campaignOpts.SetLeaderValue(leaderValue)
	opts := NewElectionManagerOptions().
		SetCampaignOptions(campaignOpts).
		SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.changeElectionState(FollowerState, LeaderState)
	require.Equal(t, mgr.electionState, LeaderState)
}

func TestElectionManagerCancelInProgressChangeNoInProgressChange(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	require.False(t, mgr.cancelInProgressChange())
}

func TestElectionManagerCancelInProgressChangeWithInProgressChange(t *testing.T) {
	var cancelled int
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.changeInProgress = true
	mgr.changeCancelFn = func() { cancelled++ }
	require.True(t, mgr.cancelInProgressChange())
	require.False(t, mgr.changeInProgress)
	require.Nil(t, mgr.changeCancelFn)
}

func TestElectionManagerLeaderToFollowerWithTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()

	leaderValue := "myself"
	leaderService := &mockLeaderService{
		leaderFn: func(electionID string) (string, error) {
			return "someone else", nil
		},
	}
	campaignOpts, err := services.NewCampaignOptions()
	require.NoError(t, err)
	campaignOpts = campaignOpts.SetLeaderValue(leaderValue)
	opts := NewElectionManagerOptions().
		SetCampaignOptions(campaignOpts).
		SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.Lock()
	mgr.electionState = LeaderState
	mgr.Unlock()
	mgr.changeWg.Add(1)
	mgr.leaderToFollower(ctx)
	mgr.changeWg.Wait()
	require.Equal(t, LeaderState, mgr.electionState)
}

func TestElectionManagerLeaderToFollowerWithLeaderErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	iter := 0
	leaderValue := "myself"
	leaderService := &mockLeaderService{
		leaderFn: func(electionID string) (string, error) {
			iter++
			if iter == 1 {
				return "", errors.New("leader service error")
			}
			if iter == 2 {
				return leaderValue, nil
			}
			return "someone else", nil
		},
	}
	campaignOpts, err := services.NewCampaignOptions()
	require.NoError(t, err)
	campaignOpts = campaignOpts.SetLeaderValue(leaderValue)
	opts := NewElectionManagerOptions().
		SetCampaignOptions(campaignOpts).
		SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.Lock()
	mgr.electionState = LeaderState
	mgr.Unlock()
	mgr.changeWg.Add(1)
	mgr.leaderToFollower(ctx)
	mgr.changeWg.Wait()
	require.Equal(t, FollowerState, mgr.electionState)
	require.Equal(t, 3, iter)
}

func TestElectionManagerIsResigned(t *testing.T) {
	leaderService := &mockLeaderService{
		campaignFn: func(
			electionID string,
			opts services.CampaignOptions,
		) (<-chan campaign.Status, error) {
			return make(chan campaign.Status), nil
		},
	}
	opts := testElectionManagerOptions(t).SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)
	require.NoError(t, mgr.Open(testShardSetID))
	require.False(t, mgr.isResigned())
	require.NoError(t, mgr.Close())
	require.True(t, mgr.isResigned())
}

func testElectionManagerOptions(t *testing.T) ElectionManagerOptions {
	campaignOpts, err := services.NewCampaignOptions()
	require.NoError(t, err)
	return NewElectionManagerOptions().SetCampaignOptions(campaignOpts)
}

type campaignFn func(
	electionID string,
	opts services.CampaignOptions,
) (<-chan campaign.Status, error)

type resignFn func(electionID string) error
type leaderFn func(electionID string) (string, error)

type mockLeaderService struct {
	campaignFn campaignFn
	resignFn   resignFn
	leaderFn   leaderFn
}

func (s *mockLeaderService) Campaign(
	electionID string,
	opts services.CampaignOptions,
) (<-chan campaign.Status, error) {
	return s.campaignFn(electionID, opts)
}

func (s *mockLeaderService) Resign(electionID string) error {
	return s.resignFn(electionID)
}

func (s *mockLeaderService) Leader(electionID string) (string, error) {
	return s.leaderFn(electionID)
}

func (s *mockLeaderService) Close() error { return nil }

type electionOpenFn func(shardSetID string) error
type electionResignFn func(ctx context.Context) error

type mockElectionManager struct {
	sync.RWMutex

	openFn        electionOpenFn
	electionState ElectionState
	resignFn      electionResignFn
}

func (m *mockElectionManager) Open(shardSetID string) error { return m.openFn(shardSetID) }
func (m *mockElectionManager) ElectionState() ElectionState {
	m.RLock()
	state := m.electionState
	m.RUnlock()
	return state
}
func (m *mockElectionManager) Resign(ctx context.Context) error { return m.resignFn(ctx) }
func (m *mockElectionManager) Close() error                     { return nil }
