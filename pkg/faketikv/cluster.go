// Copyright 2017 PingCAP, Inc.
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

package faketikv

import (
	log "github.com/Sirupsen/logrus"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
)

// ClusterInfo records all cluster information
type ClusterInfo struct {
	*core.RegionsInfo
	Nodes       map[uint64]*Node
	firstRegion *core.RegionInfo
}

// GetBootstrapInfo returns first region and it's leader store
func (c *ClusterInfo) GetBootstrapInfo() (*metapb.Store, *metapb.Region) {
	storeID := c.firstRegion.Leader.GetStoreId()
	store := c.Nodes[storeID]
	return store.Store, c.firstRegion.Region
}

func (c *ClusterInfo) nodeHealth(storeID uint64) bool {
	n, ok := c.Nodes[storeID]
	if !ok {
		return false
	}
	return n.Store.GetState() == metapb.StoreState_Up
}

func (c *ClusterInfo) stepLeader(region *core.RegionInfo) {
	if c.nodeHealth(region.Leader.GetStoreId()) {
		return
	}
	var (
		newLeaderStoreID uint64
		unhealth         int
	)

	ids := region.GetStoreIds()
	for id := range ids {
		if c.nodeHealth(id) {
			newLeaderStoreID = id
			break
		} else {
			unhealth++
		}
	}
	// TODO:records no leader region
	if unhealth > len(ids)/2 {
		return
	}
	for _, peer := range region.Peers {
		if peer.GetStoreId() == newLeaderStoreID {
			log.Info("[region %d]elect new leader: %+v,old leader: %+v", region.GetId(), peer, region.Leader)
			region.Leader = peer
			region.RegionEpoch.ConfVer++
			break
		}
	}
	c.SetRegion(region)
	c.reportRegionChange(region.GetId())
}

func (c *ClusterInfo) reportRegionChange(regionID uint64) {
	region := c.GetRegion(regionID)
	if n, ok := c.Nodes[region.Leader.GetStoreId()]; ok {
		n.reportRegionChange(region.GetId())
	}
}

func (c *ClusterInfo) stepRegions() {
	regions := c.GetRegions()
	for _, region := range regions {
		c.stepLeader(region)
	}
}

// AddTask adds task in specify node
func (c *ClusterInfo) AddTask(task Task) {
	storeID := task.TargetStoreID()
	c.Nodes[storeID].AddTask(task)
}
