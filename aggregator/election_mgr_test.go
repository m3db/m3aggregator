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
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/leader/campaign"

	"github.com/stretchr/testify/require"
)

func TestElectionStateJSONMarshal(t *testing.T) {
	for _, input := range []struct {
		state    ElectionState
		expected string
	}{
		{state: LeaderState, expected: `"leader"`},
		{state: FollowerState, expected: `"follower"`},
	} {
		b, err := json.Marshal(input.state)
		require.NoError(t, err)
		require.Equal(t, input.expected, string(b))
	}
}

func TestElectionStateJSONMarshalError(t *testing.T) {
	require.Panics(t, func() {
		json.Marshal(ElectionState(100))
	})
}

func TestElectionStateJSONUnMarshal(t *testing.T) {
	for _, input := range []struct {
		str      string
		expected ElectionState
	}{
		{str: `"leader"`, expected: LeaderState},
		{str: `"follower"`, expected: FollowerState},
	} {
		var actual ElectionState
		require.NoError(t, json.Unmarshal([]byte(input.str), &actual))
		require.Equal(t, input.expected, actual)
	}
}

func TestElectionStateJSONUnMarshalError(t *testing.T) {
	var state ElectionState
	require.Error(t, json.Unmarshal([]byte(`"foo"`), &state))
}

func TestElectionStateJSONRoundtrip(t *testing.T) {
	for _, input := range []ElectionState{LeaderState, FollowerState} {
		var actual ElectionState
		b, err := json.Marshal(input)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(b, &actual))
		require.Equal(t, input, actual)
	}
}

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
	mgr.sleepFn = func(time.Duration) {}
	mgr.Lock()
	mgr.electionState = LeaderState
	mgr.Unlock()
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
	mgr.sleepFn = func(time.Duration) {}
	mgr.Lock()
	mgr.electionState = LeaderState
	mgr.Unlock()
	require.NoError(t, mgr.Open(testShardSetID))
	require.Error(t, mgr.Resign(ctx))
	require.Equal(t, electionManagerOpen, mgr.state)
	require.NoError(t, mgr.Close())
}

func TestElectionManagerResignSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var (
		statusCh = make(chan campaign.Status)
		mgr      *electionManager
	)
	leaderService := &mockLeaderService{
		campaignFn: func(
			electionID string,
			opts services.CampaignOptions,
		) (<-chan campaign.Status, error) {
			return statusCh, nil
		},
		resignFn: func(electionID string) error {
			close(statusCh)
			go func() {
				// Simulate a delay between resignation and status change.
				time.Sleep(500 * time.Millisecond)
				mgr.Lock()
				mgr.electionState = FollowerState
				mgr.Unlock()
				mgr.notifyStateChange()
			}()
			return nil
		},
	}
	opts := testElectionManagerOptions(t).SetLeaderService(leaderService)
	mgr = NewElectionManager(opts).(*electionManager)
	mgr.sleepFn = func(time.Duration) {}
	mgr.Lock()
	mgr.electionState = LeaderState
	mgr.Unlock()
	require.NoError(t, mgr.Open(testShardSetID))

	require.NoError(t, mgr.Resign(ctx))
	require.Equal(t, electionManagerOpen, mgr.state)
	require.NoError(t, mgr.Close())
}

func TestElectionManagerCloseNotOpenOrResigned(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.state = electionManagerNotOpen
	require.Equal(t, errElectionManagerExpectOpen, mgr.Close())
}

func TestElectionManagerCloseSuccess(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.state = electionManagerOpen
	require.NoError(t, mgr.Close())
}

func TestElectionManagerStartElectionLoop(t *testing.T) {
	iter := 0
	leaderValue := "myself"
	campaignCh := make(chan campaign.Status)
	nextCampaignCh := make(chan campaign.Status)
	leaderService := &mockLeaderService{
		campaignFn: func(
			electionID string,
			opts services.CampaignOptions,
		) (<-chan campaign.Status, error) {
			iter++
			if iter == 1 {
				return campaignCh, nil
			}
			return nextCampaignCh, nil
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
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		mgr.Lock()
		mgr.state = electionManagerResigning
		mgr.resignWg = &sync.WaitGroup{}
		mgr.resignWg.Add(1)
		mgr.Unlock()
		campaignCh <- campaign.NewStatus(campaign.Leader)
		close(campaignCh)

		// Create a bit of lag between when campaign channel is closed
		// and when the resign completes.
		time.Sleep(500 * time.Millisecond)
		mgr.resignWg.Done()

		mgr.Lock()
		mgr.state = electionManagerOpen
		mgr.resignWg = nil
		mgr.Unlock()
	}()

	for {
		if mgr.ElectionState() == LeaderState {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	wg.Wait()

	// Verifying we are still electing.
	nextCampaignCh <- campaign.NewStatus(campaign.Leader)

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
