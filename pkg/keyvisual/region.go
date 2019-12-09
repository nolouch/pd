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
	"reflect"
	"time"
	"unsafe"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/keyvisual/matrix"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/core"
	"go.uber.org/zap"
)

type statTag int

const (
	WrittenBytesTag statTag = iota
	ReadBytesTag
	WrittenKeysTag
	ReadKeysTag
	IntegrationTag
)

const valuesListLen = 4

func getTag(typ string) statTag {
	switch typ {
	case "":
		return IntegrationTag
	case "written_bytes":
		return WrittenBytesTag
	case "read_bytes":
		return ReadBytesTag
	case "written_keys":
		return WrittenKeysTag
	case "read_keys":
		return ReadKeysTag
	default:
		return WrittenBytesTag
	}
}

// Fixme: StartKey may not be equal to the EndKey of the previous region
func getKeys(regions []*core.RegionInfo) []string {
	keys := make([]string, len(regions)+1)
	keys[0] = String(regions[0].GetStartKey())
	endKeys := keys[1:]
	for i, region := range regions {
		endKeys[i] = String(region.GetEndKey())
	}
	return keys
}

func getValues(regions []*core.RegionInfo, tag statTag) []uint64 {
	values := make([]uint64, len(regions))
	switch tag {
	case WrittenBytesTag:
		for i, region := range regions {
			values[i] = region.GetBytesWritten()
		}
	case ReadBytesTag:
		for i, region := range regions {
			values[i] = region.GetBytesRead()
		}
	case WrittenKeysTag:
		for i, region := range regions {
			values[i] = region.GetKeysWritten()
		}
	case ReadKeysTag:
		for i, region := range regions {
			values[i] = region.GetKeysRead()
		}
	case IntegrationTag:
		for i, region := range regions {
			values[i] = region.GetBytesWritten() + region.GetBytesRead()
		}
	default:
		panic("unreachable")
	}
	return values
}

func filter(axis *matrix.Axis, tags ...statTag) matrix.Axis {
	valuesList := make([][]uint64, len(tags))
	for i, tag := range tags {
		var values []uint64
		switch tag {
		case WrittenBytesTag:
			values = axis.ValuesList[0]
		case ReadBytesTag:
			values = axis.ValuesList[1]
		case WrittenKeysTag:
			values = axis.ValuesList[2]
		case ReadKeysTag:
			values = axis.ValuesList[3]
		case IntegrationTag:
			values = make([]uint64, len(axis.ValuesList[0]))
			for j := range values {
				values[j] = axis.ValuesList[0][j] + axis.ValuesList[1][j]
			}
		}
		valuesList[i] = values
	}
	return matrix.CreateAxis(axis.Keys, valuesList...)
}

// TODO: pre-compact based on IntegrationTag
func toAxis(regions []*core.RegionInfo) matrix.Axis {
	regionsLen := len(regions)
	if regionsLen <= 0 {
		panic("At least one RegionInfo")
	}
	keys := getKeys(regions)
	valuesList := make([][]uint64, valuesListLen)
	valuesList[0] = getValues(regions, WrittenBytesTag)
	valuesList[1] = getValues(regions, ReadBytesTag)
	valuesList[2] = getValues(regions, WrittenKeysTag)
	valuesList[3] = getValues(regions, ReadKeysTag)
	return matrix.CreateAxis(keys, valuesList...)
}

func scanRegions(cluster *server.RaftCluster) ([]*core.RegionInfo, time.Time) {
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

	log.Info("Update key visual regions", zap.Int("total-length", len(regions)))
	return regions, time.Now()
}

func String(b []byte) (s string) {
	if len(b) == 0 {
		return ""
	}
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Len
	return
}
