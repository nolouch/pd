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
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/core"
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
	return regions
}
