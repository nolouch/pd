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
	IntegrationTag statTag = iota
	WrittenBytesTag
	ReadBytesTag
	WrittenKeysTag
	ReadKeysTag
)

func getTag(typ string) statTag {
	switch typ {
	case "":
		return IntegrationTag
	case "integration":
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

func (tag statTag) String() string {
	switch tag {
	case IntegrationTag:
		return "integration"
	case WrittenBytesTag:
		return "written_bytes"
	case ReadBytesTag:
		return "read_bytes"
	case WrittenKeysTag:
		return "written_keys"
	case ReadKeysTag:
		return "read_keys"
	default:
		panic("unreachable")
	}
}

var storageTags = []statTag{WrittenBytesTag, ReadBytesTag, WrittenKeysTag, ReadKeysTag}
var responseTags = append([]statTag{IntegrationTag}, storageTags...)

func getDisplayTags(baseTag statTag) []string {
	displayTags := make([]string, len(responseTags))
	for i, tag := range responseTags {
		displayTags[i] = tag.String()
		if tag == baseTag {
			displayTags[0], displayTags[i] = displayTags[i], displayTags[0]
		}
	}
	return displayTags
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

func (s *Stat) StorageAxis(regions []*core.RegionInfo) matrix.Axis {
	regionsLen := len(regions)
	if regionsLen <= 0 {
		panic("At least one RegionInfo")
	}

	keys := getKeys(regions)
	valuesList := make([][]uint64, len(responseTags))
	for i, tag := range responseTags {
		valuesList[i] = getValues(regions, tag)
	}
	matrix.SaveKeys(keys)
	preAxis := matrix.CreateAxis(keys, valuesList)

	target := maxDisplayY
	focusAxis := preAxis.Focus(s.strategy, 1, len(keys)/target, target)

	// responseTags -> storageTags
	var storageValuesList [][]uint64
	storageValuesList = append(storageValuesList, focusAxis.ValuesList[1:]...)
	log.Info("New StorageAxis", zap.Int("region length", len(regions)), zap.Int("focus keys length", len(focusAxis.Keys)))
	return matrix.CreateAxis(focusAxis.Keys, storageValuesList)
}

func intoResponseAxis(storageAxis matrix.Axis, baseTag statTag) matrix.Axis {
	// add integration values
	valuesList := make([][]uint64, 1, len(responseTags))
	writtenBytes := storageAxis.ValuesList[0]
	readBytes := storageAxis.ValuesList[1]
	integration := make([]uint64, len(writtenBytes))
	for i := range integration {
		integration[i] = writtenBytes[i] + readBytes[i]
	}
	valuesList[0] = integration
	valuesList = append(valuesList, storageAxis.ValuesList...)
	// swap baseTag
	for i, tag := range responseTags {
		if tag == baseTag {
			valuesList[0], valuesList[i] = valuesList[i], valuesList[0]
			return matrix.CreateAxis(storageAxis.Keys, valuesList)
		}
	}
	panic("unreachable")
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

// TODO: use server/core/region.go version
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
