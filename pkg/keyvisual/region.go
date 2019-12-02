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

package keyvisual

import (
	"encoding/hex"
	"encoding/json"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api"
	"github.com/pingcap/pd/server/core"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"time"
)

func scanRegions(cluster *server.RaftCluster) []*core.RegionInfo {
	var key []byte
	regions := make([]*core.RegionInfo, 0, 1024)
	for {
		rs := cluster.ScanRegions(key, []byte(""), 1024)
		length := len(rs)
		if length == 0 {
			break
		}
		regions = append(regions, rs...)

		key = rs[length-1].GetEndKey()
		if len(key) == 0 {
			break
		}
	}

	log.Info("Update keyvisual regions", zap.Int("total-length", len(regions)))
	return regions
}

// read from file
func scanRegionsFromFile(cluster *server.RaftCluster) []*core.RegionInfo {
	var res []*core.RegionInfo
	fileName := fileNow.Format("./data/20060102-15-04.json")
	fileNow = fileNow.Add(fileDelta)
	jsonFile, err := os.Open(fileName)
	if err != nil {
		return res
	}
	defer jsonFile.Close()
	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return res
	}
	var apiRes api.RegionsInfo
	json.Unmarshal(byteValue, &apiRes)
	res = make([]*core.RegionInfo, len(apiRes.Regions))
	for i, r := range apiRes.Regions {
		res[i] = toCoreRegion(r)
	}
	return res
}

var fileNow = time.Unix(1575129600, 0) // 2019.12.01 00:00
var fileDelta = time.Minute

func toCoreRegion(aRegion *api.RegionInfo) *core.RegionInfo {
	startKey, _ := hex.DecodeString(aRegion.StartKey)
	endKey, _ := hex.DecodeString(aRegion.EndKey)
	meta := &metapb.Region{
		Id:          aRegion.ID,
		StartKey:    startKey,
		EndKey:      endKey,
		RegionEpoch: aRegion.RegionEpoch,
		Peers:       aRegion.Peers,
	}
	return core.NewRegionInfo(meta, aRegion.Leader,
		core.SetApproximateKeys(aRegion.ApproximateKeys),
		core.SetApproximateSize(aRegion.ApproximateSize),
		core.WithPendingPeers(aRegion.PendingPeers),
		core.WithDownPeers(aRegion.DownPeers),
		core.SetWrittenBytes(aRegion.WrittenBytes),
		core.SetWrittenKeys(aRegion.WrittenKeys),
		core.SetReadBytes(aRegion.ReadBytes),
		core.SetReadKeys(aRegion.ReadKeys),
	)
}
