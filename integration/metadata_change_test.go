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
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3metrics/metric/aggregated"

	"github.com/stretchr/testify/require"
)

func TestMetadataChangeWithPoliciesList(t *testing.T) {
	oldMetadata := metadataUnion{
		mType:        policiesListType,
		policiesList: testPoliciesList,
	}
	newMetadata := metadataUnion{
		mType:        policiesListType,
		policiesList: testUpdatedPoliciesList,
	}
	testMetadataChange(t, oldMetadata, newMetadata)
}

func TestMetadataChangeWithStagedMetadatas(t *testing.T) {
	oldMetadata := metadataUnion{
		mType:           stagedMetadatasType,
		stagedMetadatas: testStagedMetadatas,
	}
	newMetadata := metadataUnion{
		mType:           stagedMetadatasType,
		stagedMetadatas: testUpdatedStagedMetadatas,
	}
	testMetadataChange(t, oldMetadata, newMetadata)
}

func testMetadataChange(t *testing.T, oldMetadata, newMetadata metadataUnion) {
	if testing.Short() {
		t.SkipNow()
	}

	// Test setup.
	testSetup := newTestSetup(t, newTestOptions())
	defer testSetup.close()

	testSetup.aggregatorOpts =
		testSetup.aggregatorOpts.
			SetEntryCheckInterval(time.Second)

	// Start the server.
	log := testSetup.aggregatorOpts.InstrumentOptions().Logger()
	log.Info("test metadata change")
	require.NoError(t, testSetup.startServer())
	log.Info("server is now up")
	require.NoError(t, testSetup.waitUntilLeader())
	log.Info("server is now the leader")

	var (
		idPrefix = "foo"
		numIDs   = 100
		start    = testSetup.getNowFn()
		middle   = start.Add(4 * time.Second)
		end      = start.Add(10 * time.Second)
		interval = time.Second
	)
	client := testSetup.newClient()
	require.NoError(t, client.connect())
	defer client.close()

	ids := generateTestIDs(idPrefix, numIDs)
	dataset1 := generateTestDataset(start, middle, interval, ids, roundRobinMetricTypeFn)
	dataset2 := generateTestDataset(middle, end, interval, ids, roundRobinMetricTypeFn)
	inputs := []struct {
		dataset  testDataset
		metadata metadataUnion
	}{
		{
			dataset:  dataset1,
			metadata: oldMetadata,
		},
		{
			dataset:  dataset2,
			metadata: newMetadata,
		},
	}
	for _, input := range inputs {
		for _, data := range input.dataset {
			testSetup.setNowFn(data.timestamp)
			for _, mu := range data.metrics {
				if input.metadata.mType == policiesListType {
					require.NoError(t, client.writeMetricWithPoliciesList(mu, input.metadata.policiesList))
				} else {
					require.NoError(t, client.writeMetricWithMetadatas(mu, input.metadata.stagedMetadatas))
				}
			}
			require.NoError(t, client.flush())

			// Give server some time to process the incoming packets.
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Move time forward and wait for ticking to happen. The sleep time
	// must be the longer than the lowest resolution across all policies.
	finalTime := end.Add(time.Second)
	testSetup.setNowFn(finalTime)
	time.Sleep(6 * time.Second)

	// Stop the server.
	require.NoError(t, testSetup.stopServer())
	log.Info("server is now down")

	// Validate results.
	var expected []aggregated.MetricWithStoragePolicy
	for _, input := range inputs {
		expected = append(expected, computeExpectedResults(t, finalTime, input.dataset, input.metadata, testSetup.aggregatorOpts)...)
	}
	sort.Sort(byTimeIDPolicyAscending(expected))
	actual := testSetup.sortedResults()
	require.Equal(t, expected, actual)
}
