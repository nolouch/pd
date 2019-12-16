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
	"io/ioutil"
	"os"
	"sort"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/api"
	"github.com/pingcap/pd/server/core"
	"go.uber.org/zap"
)

var fileNextTime = time.Unix(1574992800, 0) // 2019.11.29 10:00
var fileEndTime = time.Unix(1575064800, 0)  // 2019.11.30 06:00
var fileTimeDelta = time.Minute

func (s *Service) updateStatFromFiles() {
	log.Info("Keyvisual load files from", zap.Time("start-time", fileNextTime))
	now := time.Now()
	for {
		regions, endTime := scanRegionsFromFile()
		newTime := now.Add(endTime.Sub(fileEndTime))
		s.stats.Append(regions, newTime)
		if endTime.After(fileEndTime) {
			break
		}
	}
	log.Info("Keyvisual load files to", zap.Time("end-time", fileNextTime))
	log.Info("Keyvisual load all files")
}

func scanRegionsFromFile() ([]*core.RegionInfo, time.Time) {
	var res []*core.RegionInfo
	fileNow := fileNextTime
	fileNextTime = fileNow.Add(fileTimeDelta)
	fileName := fileNow.Format("./data/20060102-15-04.json")
	if jsonFile, err := os.Open(fileName); err == nil {
		defer jsonFile.Close()
		if byteValue, err := ioutil.ReadAll(jsonFile); err == nil {
			var apiRes api.RegionsInfo
			json.Unmarshal(byteValue, &apiRes)
			regions := apiRes.Regions
			sort.Slice(regions, func(i, j int) bool {
				return regions[i].StartKey < regions[j].StartKey
			})
			res = make([]*core.RegionInfo, len(regions))
			for i, r := range regions {
				res[i] = toCoreRegion(r)
			}
		}
	}
	return res, fileNow
}

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
