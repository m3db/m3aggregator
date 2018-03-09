// +build integration

// Copyright (c) 2016 Uber Technologies, Inc.
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

package integration

import (
	"math/rand"
	"testing"
	"time"

	"github.com/m3db/m3metrics/metric/unaggregated"

	"github.com/stretchr/testify/require"
)

func TestMultiClientOneType(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// Test setup.
	testSetup := newTestSetup(t, newTestOptions())
	defer testSetup.close()

	testSetup.aggregatorOpts =
		testSetup.aggregatorOpts.
			SetEntryCheckInterval(time.Second).
			SetMinFlushInterval(0)

	// Start the server.
	log := testSetup.aggregatorOpts.InstrumentOptions().Logger()
	log.Info("test multiple clients sending one type of metrics")
	require.NoError(t, testSetup.startServer())
	log.Info("server is now up")
	require.NoError(t, testSetup.waitUntilLeader())
	log.Info("server is now the leader")

	var (
		idPrefix   = "foo"
		numIDs     = 100
		start      = testSetup.getNowFn()
		stop       = start.Add(10 * time.Second)
		interval   = time.Second
		numClients = 10
		clients    = make([]*client, numClients)
	)
	for i := 0; i < numClients; i++ {
		clients[i] = testSetup.newClient()
		require.NoError(t, clients[i].connect())
		defer clients[i].close()
	}

	ids := generateTestIDs(idPrefix, numIDs)
	typeFn := constantMetryTypeFnFactory(unaggregated.CounterType)
	input := generateTestData(start, stop, interval, ids, typeFn, testPoliciesList)
	for _, data := range input.dataset {
		testSetup.setNowFn(data.timestamp)
		for _, mu := range data.metrics {
			// Randomly pick one client to write the metric.
			client := clients[rand.Int63n(int64(numClients))]
			require.NoError(t, client.write(mu, input.policiesList))
		}
		for _, client := range clients {
			require.NoError(t, client.flush())
		}

		// Give server some time to process the incoming packets.
		time.Sleep(100 * time.Millisecond)
	}

	// Move time forward and wait for ticking to happen. The sleep time
	// must be the longer than the lowest resolution across all policies.
	finalTime := stop.Add(time.Second)
	testSetup.setNowFn(finalTime)
	time.Sleep(4 * time.Second)

	// Stop the server.
	require.NoError(t, testSetup.stopServer())
	log.Info("server is now down")

	// Validate results.
	expected := toExpectedResults(t, finalTime, input, testSetup.aggregatorOpts)
	actual := testSetup.sortedResults()
	require.Equal(t, expected, actual)
}
