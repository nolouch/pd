// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server/schedule"
)

var _ = Suite(&testNameSpaceCheckerSuite{})

type testNameSpaceCheckerSuite struct{}

func (s *testNameSpaceCheckerSuite) TestCheckerWhenAbleToMove(c *C) {

	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	tc.addRegionStore(1, 3)
	tc.addRegionStore(2, 1)
	tc.addRegionStore(3, 1)
	tc.addRegionStore(4, 2)
	tc.addRegionStore(5, 1)
	tc.addRegionStore(6, 1)

	//| region_id | leader_sotre | follower_store | follower_store |
	//|-----------|--------------|----------------|----------------|
	//|     1     |       1      |        3       |       4        |
	//|     2     |       1      |        2       |       5        |
	//|     3     |       1      |        4       |       6        |
	tc.addLeaderRegion(1, 1, 6, 4)
	tc.addLeaderRegion(2, 1, 2, 5)
	tc.addLeaderRegion(3, 1, 4, 6)

	// init classifier & namespace
	classifier := newMapClassifer()
	classifier.setRegion(1, "ns1")
	classifier.setRegion(2, "ns2")
	classifier.setRegion(3, "ns3")

	classifier.setStore(1, "ns1")
	classifier.setStore(2, "ns1")
	classifier.setStore(3, "ns2")
	classifier.setStore(4, "ns2")
	classifier.setStore(5, "ns3")
	classifier.setStore(6, "ns1")

	_, scheduleOption := newTestScheduleConfig()

	namespaceChecker := schedule.NewNamespaceChecker(scheduleOption, cluster, classifier)

	region := tc.GetRegion(1)
	op := namespaceChecker.Check(region)
	// This checker finds the region1 peer on store4 and moves it to store 2
	checkTransferPeer(c, op, 4, 2)

}


func (s *testNameSpaceCheckerSuite) TestCheckerWhenNoNeedToMove(c *C) {

	cluster := newClusterInfo(newMockIDAllocator())
	tc := newTestClusterInfo(cluster)

	tc.addRegionStore(1, 3)
	tc.addRegionStore(2, 1)
	tc.addRegionStore(3, 1)
	tc.addRegionStore(4, 2)
	tc.addRegionStore(5, 1)
	tc.addRegionStore(6, 1)

	//| region_id | leader_sotre | follower_store | follower_store |
	//|-----------|--------------|----------------|----------------|
	//|     1     |       1      |        3       |       4        |
	//|     2     |       1      |        2       |       5        |
	//|     3     |       1      |        4       |       6        |

	tc.addLeaderRegion(1, 1, 2, 6)
	tc.addLeaderRegion(2, 1, 3, 4)
	tc.addLeaderRegion(3, 1, 4, 6)

	// init classifier & namespace
	classifier := newMapClassifer()
	classifier.setRegion(1, "ns1")
	classifier.setRegion(2, "ns2")
	classifier.setRegion(3, "ns3")

	classifier.setStore(1, "ns1")
	classifier.setStore(2, "ns1")
	classifier.setStore(3, "ns2")
	classifier.setStore(4, "ns2")
	classifier.setStore(5, "ns3")
	classifier.setStore(6, "ns1")

	_, scheduleOption := newTestScheduleConfig()

	namespaceChecker := schedule.NewNamespaceChecker(scheduleOption, cluster, classifier)

	// region1 is not a magic number, it names after the region with regionID 1
	region1 := tc.GetRegion(1)
	// all peers belong to region1 are in the right namespace so no need to move
	c.Assert(namespaceChecker.Check(region1), IsNil)

	region2 := tc.GetRegion(2)
	// all stores belongs to ns2 contains at least one peer of region2, so no need to move
	c.Assert(namespaceChecker.Check(region2), IsNil)


}