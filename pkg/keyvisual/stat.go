// Copyright 2019 PingCAP, Inc.
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
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/keyvisual/matrix"
	"github.com/pingcap/pd/server/core"
	"go.uber.org/zap"
)

const (
	WrittenBytesTag matrix.ValueTag = iota + 1
	ReadBytesTag
	WrittenKeysTag
	ReadKeysTag
)

const MaxDisplayY = 1500

func GetTag(typ string) matrix.ValueTag {
	switch typ {
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

type statUnit struct {
	WrittenBytes uint64 `json:"written_bytes"`
	ReadBytes    uint64 `json:"read_bytes"`
	WrittenKeys  uint64 `json:"written_keys"`
	ReadKeys     uint64 `json:"read_keys"`
}

var zeroStatUnit matrix.Value = &statUnit{
	WrittenBytes: 0,
	ReadBytes:    0,
	WrittenKeys:  0,
	ReadKeys:     0,
}

func (v *statUnit) Clone() matrix.Value {
	statUnitClone := *v
	return &statUnitClone
}

func (v *statUnit) Equal(other matrix.Value) bool {
	another := other.(*statUnit)
	return *v == *another
}

func newStatUnit(r *core.RegionInfo) *statUnit {
	return &statUnit{
		WrittenBytes: r.GetBytesWritten(),
		ReadBytes:    r.GetBytesRead(),
		WrittenKeys:  r.GetKeysWritten(),
		ReadKeys:     r.GetKeysRead(),
	}
}

func (v *statUnit) Split(count int) matrix.Value {
	countU64 := uint64(count)
	res := *v
	res.ReadKeys /= countU64
	res.ReadBytes /= countU64
	res.WrittenKeys /= countU64
	res.WrittenBytes /= countU64
	return &res
}

func (v *statUnit) GetValue(tag matrix.ValueTag) uint64 {
	switch tag {
	case WrittenBytesTag:
		return v.WrittenBytes
	case ReadBytesTag:
		return v.ReadBytes
	case WrittenKeysTag:
		return v.WrittenBytes
	case ReadKeysTag:
		return v.ReadKeys
	case matrix.INTEGRATION:
		return v.ReadBytes + v.WrittenBytes
	default:
		return v.WrittenBytes
	}
}

func (v *statUnit) Merge(other matrix.Value) {
	v2 := other.(*statUnit)
	v.WrittenBytes += v2.WrittenBytes
	v.WrittenKeys += v2.WrittenKeys
	v.ReadBytes += v2.ReadBytes
	v.ReadKeys += v2.ReadKeys
}

type layerStat struct {
	startTime time.Time
	ring      []*matrix.DiscreteAxis
	head      int
	tail      int
	empty     bool
	len       int
	// Hierarchical mechanism
	compactRatio  int
	nextLayerStat *layerStat // if nextLayerStat is nil, the layer is last layer
}

func newLayerStat(ratio int, len int) *layerStat {
	if ratio == 0 || len == 0 {
		return &layerStat{
			startTime:     time.Now(),
			ring:          make([]*matrix.DiscreteAxis, 0),
			head:          0,
			tail:          0,
			empty:         true,
			len:           0,
			compactRatio:  0,
			nextLayerStat: nil,
		}
	}

	return &layerStat{
		startTime:     time.Now(),
		ring:          make([]*matrix.DiscreteAxis, len, len),
		head:          0,
		tail:          0,
		empty:         true,
		len:           len,
		compactRatio:  ratio,
		nextLayerStat: nil,
	}
}

// Append appends a key axis to layerStat.
func (s *layerStat) Append(axis *matrix.DiscreteAxis) {
	//if s.nextLayerStat == nil {
	//	// the last layer do not limit the capcity.
	//	s.ring = append(s.ring, axis)
	//	s.tail++
	//	s.empty = false
	//	return
	//}

	if s.head == s.tail && !s.empty {
		// log.S().Info(s.head, s.tail)
		// compress data
		if s.nextLayerStat == nil {
			s.head = (s.head + 1) % s.len
			s.ring[s.tail] = axis
			s.tail = (s.tail + 1) % s.len
			return
		}
		plane := new(matrix.DiscretePlane)
		plane.StartTime = s.startTime
		plane.Axes = make([]*matrix.DiscreteAxis, s.compactRatio, s.compactRatio)
		for i := 0; i < s.compactRatio; i++ {
			plane.Axes[i] = s.ring[s.head]
			s.head = (s.head + 1) % s.len
		}
		compactAxis, _ := plane.Compact(zeroStatUnit)
		s.startTime = compactAxis.EndTime
		s.nextLayerStat.Append(compactAxis)
	}

	s.ring[s.tail] = axis
	s.empty = false
	s.tail = (s.tail + 1) % s.len
}

// Search binary search the key axis with the time.
func (s *layerStat) Search(t time.Time) (int, bool) {
	if s.empty {
		return -1, false
	}
	var l, r, end, size int
	if s.nextLayerStat == nil {
		size = len(s.ring)
		l, r, end = 0, size, size
	} else {
		l, r, size = s.head, s.tail, s.len
		if r <= l {
			r += size
		}
		end = r
	}
	for l < r {
		m := (l + r) / 2
		if s.ring[m%size].EndTime.Before(t) {
			l = m + 1
		} else {
			r = m
		}
	}
	if l == end {
		return (end - 1) % size, false
	} else {
		return l % size, true
	}
}

// Range gets the specify discrete plance in the time range.
func (s *layerStat) Range(startTime time.Time, endTime time.Time) *matrix.DiscretePlane {
	startIndex, ok := s.Search(startTime)
	if !ok {
		if s.nextLayerStat != nil {
			return s.nextLayerStat.Range(startTime, endTime)
		}
		return nil
	}
	endIndex, _ := s.Search(endTime)
	endIndex++

	// generate plane
	plane := new(matrix.DiscretePlane)
	plane.Axes = make([]*matrix.DiscreteAxis, 0)
	if startIndex == s.head {
		plane.StartTime = s.startTime
	} else if startIndex > 0 {
		plane.StartTime = s.ring[startIndex-1].EndTime
	} else {
		plane.StartTime = s.ring[s.len-1].EndTime
	}
	if endIndex > startIndex {
		plane.Axes = append(plane.Axes, s.ring[startIndex:endIndex]...)
	} else {
		plane.Axes = append(plane.Axes, s.ring[startIndex:s.len]...)
		plane.Axes = append(plane.Axes, s.ring[0:endIndex]...)
	}
	if s.nextLayerStat != nil {
		nextPlane := s.nextLayerStat.Range(startTime, endTime)
		if nextPlane != nil {
			nextPlane.Axes = append(nextPlane.Axes, plane.Axes...)
			return nextPlane
		}
	}
	return plane
}

type Stat struct {
	sync.RWMutex
	layers []*layerStat
}

type LayerConfig struct {
	Len   int
	Ratio int
}

type LayersConfig []LayerConfig

func NewStat(conf LayersConfig) *Stat {
	layers := make([]*layerStat, 0, len(conf))
	for i, c := range conf {
		layers = append(layers, newLayerStat(c.Ratio, c.Len))
		if i > 0 {
			layers[i-1].nextLayerStat = layers[i]
		}
	}
	return &Stat{
		layers: layers,
	}
}

// Append appends the all region information to statistics.
func (s *Stat) Append(regions []*core.RegionInfo) {
	if len(regions) == 0 {
		return
	}

	axis := newDiscreteAxis(regions)
	s.Lock()
	defer s.Unlock()
	s.layers[0].Append(axis)
}

func newDiscreteAxis(regions []*core.RegionInfo) *matrix.DiscreteAxis {
	endTime := time.Now()
	if len(regions) == 0 {
		return matrix.NewEmptyAxis(endTime, "", "", zeroStatUnit)
	}
	axis := &matrix.DiscreteAxis{
		StartKey: string(regions[0].GetStartKey()),
		EndTime:  endTime,
	}
	for _, info := range regions {
		if len(info.GetEndKey()) == 0 {
		}
		line := &matrix.Line{
			EndKey: string(info.GetEndKey()),
			Value:  newStatUnit(info),
		}
		axis.Lines = append(axis.Lines, line)
	}
	// TODO: Calculate reasonable parameters
	// axis.DeNoise(matrix.INTEGRATION, 1, 50, 5000)
	return axis
}

func (s *Stat) RangeMatrix(startTime time.Time, endTime time.Time, startKey string, endKey string, tag matrix.ValueTag) *matrix.Matrix {
	s.RLock()
	rangeTimePlane := s.layers[0].Range(startTime, endTime)
	s.RUnlock()
	if rangeTimePlane == nil {
		return nil
	}
	tempMaxRow := 4 * MaxDisplayY
	for i := 0; i < len(rangeTimePlane.Axes); i++ {
		axis := rangeTimePlane.Axes[i].Range(startKey, endKey, zeroStatUnit).Clone(nil)
		axis.DeNoise(matrix.INTEGRATION, 1, len(axis.Lines)/tempMaxRow, tempMaxRow)
		rangeTimePlane.Axes[i] = axis
	}
	log.Info("got range tiem plane", zap.Int("total-time-length", len(rangeTimePlane.Axes)))
	newMatrix := rangeTimePlane.Pixel(MaxDisplayY, tag, zeroStatUnit)
	return newMatrix
	// Fixme: use tidb
	//return RangeTableID(newMatrix)
}
