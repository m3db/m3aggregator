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
	mgr.status = electionManagerOpen
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

func TestElectionManagerElectionStatus(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.Lock()
	mgr.electionStatus = LeaderStatus
	mgr.Unlock()
	require.Equal(t, LeaderStatus, mgr.ElectionStatus())
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
	require.Equal(t, electionManagerOpen, mgr.status)
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
	mgr.electionStatus = LeaderStatus
	mgr.Unlock()
	require.NoError(t, mgr.Open(testShardSetID))
	require.Error(t, mgr.Resign(ctx))
	require.Equal(t, electionManagerOpen, mgr.status)
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
	mgr.electionStatus = LeaderStatus
	mgr.Unlock()
	require.NoError(t, mgr.Open(testShardSetID))

	// Update election status asynchronously.
	var updated bool
	go func() {
		time.Sleep(500 * time.Millisecond)
		mgr.Lock()
		updated = true
		mgr.electionStatus = FollowerStatus
		mgr.Unlock()
	}()

	require.NoError(t, mgr.Resign(ctx))
	require.Equal(t, electionManagerResigned, mgr.status)
	require.True(t, updated)
	require.NoError(t, mgr.Close())
}

func TestElectionManagerCloseNotOpenOrResigned(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.status = electionManagerNotOpen
	require.Equal(t, errElectionManagerExpectOpenOrResigned, mgr.Close())
}

func TestElectionManagerCloseSuccess(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.status = electionManagerOpen
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
	require.Equal(t, FollowerStatus, mgr.ElectionStatus())

	// Same status is a no op.
	campaignCh <- campaign.NewStatus(campaign.Follower)
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, FollowerStatus, mgr.ElectionStatus())

	// Follower to leader.
	campaignCh <- campaign.NewStatus(campaign.Leader)
	for {
		if mgr.ElectionStatus() == LeaderStatus {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Leader to follower
	campaignCh <- campaign.NewStatus(campaign.Follower)
	for {
		if mgr.ElectionStatus() == FollowerStatus {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	require.NoError(t, mgr.Close())
}

func TestElectionManagerChangeElectionStatusSameStatusNotLeader(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.changeElectionStatus(FollowerStatus, FollowerStatus)
	require.Equal(t, mgr.electionStatus, FollowerStatus)
}

func TestElectionManagerChangeElectionStatusSameStatusLeaderWithInProgressChange(t *testing.T) {
	var cancelled int
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.changeInProgress = true
	mgr.changeCancelFn = func() { cancelled++ }
	mgr.changeElectionStatus(LeaderStatus, LeaderStatus)
	require.Equal(t, mgr.electionStatus, LeaderStatus)
	require.False(t, mgr.changeInProgress)
	require.Nil(t, mgr.changeCancelFn)
}

func TestElectionManagerChangeElectionStatusFollowerToLeader(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.changeElectionStatus(FollowerStatus, LeaderStatus)
	require.Equal(t, mgr.electionStatus, LeaderStatus)
}

func TestElectionManagerChangeElectionStatusLeaderToFollower(t *testing.T) {
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
	mgr.changeElectionStatus(FollowerStatus, LeaderStatus)
	require.Equal(t, mgr.electionStatus, LeaderStatus)
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
	mgr.electionStatus = LeaderStatus
	mgr.Unlock()
	mgr.changeWg.Add(1)
	mgr.leaderToFollower(ctx)
	mgr.changeWg.Wait()
	require.Equal(t, LeaderStatus, mgr.electionStatus)
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
	mgr.electionStatus = LeaderStatus
	mgr.Unlock()
	mgr.changeWg.Add(1)
	mgr.leaderToFollower(ctx)
	mgr.changeWg.Wait()
	require.Equal(t, FollowerStatus, mgr.electionStatus)
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

	openFn         electionOpenFn
	electionStatus ElectionStatus
	resignFn       electionResignFn
}

func (m *mockElectionManager) Open(shardSetID string) error { return m.openFn(shardSetID) }
func (m *mockElectionManager) ElectionStatus() ElectionStatus {
	m.RLock()
	status := m.electionStatus
	m.RUnlock()
	return status
}
func (m *mockElectionManager) Resign(ctx context.Context) error { return m.resignFn(ctx) }
func (m *mockElectionManager) Close() error                     { return nil }
