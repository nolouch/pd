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
	"sort"
	"sync"
	"time"

	"github.com/pingcap/pd/pkg/keyvisual/matrix"
	"github.com/pingcap/pd/server/core"
)

type LayerConfig struct {
	Len   int
	Ratio int
}

type layerStat struct {
	StartTime time.Time
	EndTime   time.Time
	RingAxes  []matrix.Axis
	RingTimes []time.Time
	Head      int
	Tail      int
	Empty     bool
	Len       int
	// Hierarchical mechanism
	Strategy matrix.Strategy
	Ratio    int
	Next     *layerStat
}

func newLayerStat(conf LayerConfig, strategy matrix.Strategy, startTime time.Time) *layerStat {
	return &layerStat{
		StartTime: startTime,
		EndTime:   startTime,
		RingAxes:  make([]matrix.Axis, conf.Len),
		RingTimes: make([]time.Time, conf.Len),
		Head:      0,
		Tail:      0,
		Empty:     true,
		Len:       conf.Len,
		Strategy:  strategy,
		Ratio:     conf.Ratio,
		Next:      nil,
	}
}

// Merge ratio axes and append to next layerStat
func (s *layerStat) Reduce() {
	if s.Ratio == 0 || s.Next == nil {
		s.StartTime = s.RingTimes[s.Head]
		s.Head = (s.Head + 1) % s.Len
		return
	}

	times := make([]time.Time, 0, s.Ratio+1)
	times = append(times, s.StartTime)
	axes := make([]matrix.Axis, 0, s.Ratio)

	for i := 0; i < s.Ratio; i++ {
		s.StartTime = s.RingTimes[s.Head]
		times = append(times, s.StartTime)
		axes = append(axes, s.RingAxes[s.Head])
		s.Head = (s.Head + 1) % s.Len
	}

	plane := matrix.CreatePlane(times, axes)
	newAxis := plane.Compact(s.Strategy)
	s.Next.Append(newAxis, s.StartTime)
}

// Append appends a key axis to layerStat.
func (s *layerStat) Append(axis matrix.Axis, endTime time.Time) {
	if s.Head == s.Tail && !s.Empty {
		s.Reduce()
	}
	s.RingAxes[s.Tail] = axis
	s.RingTimes[s.Tail] = endTime
	s.Empty = false
	s.EndTime = endTime
	s.Tail = (s.Tail + 1) % s.Len
}

// Range gets the specify discrete plance in the time range.
func (s *layerStat) Range(startTime, endTime time.Time, times []time.Time, axes []matrix.Axis) ([]time.Time, []matrix.Axis) {
	if s.Next != nil {
		times, axes = s.Next.Range(startTime, endTime, times, axes)
	}
	if s.Empty {
		return times, axes
	}
	if !(startTime.Before(s.EndTime) && endTime.After(s.StartTime)) {
		return times, axes
	}

	size := s.Tail - s.Head
	if size <= 0 {
		size += s.Len
	}
	start := sort.Search(size, func(i int) bool {
		return s.RingTimes[(s.Head+i)%s.Len].After(startTime)
	})
	end := sort.Search(size, func(i int) bool {
		return !s.RingTimes[(s.Head+i)%s.Len].Before(endTime)
	})
	if end != size {
		end++
	}

	n := end - start
	start = (s.Head + start) % s.Len

	// add StartTime
	if len(times) == 0 {
		if start == s.Head {
			times = append(times, s.StartTime)
		} else {
			times = append(times, s.RingTimes[(start-1+s.Len)%s.Len])
		}
	}

	if start+n <= s.Len {
		times = append(times, s.RingTimes[start:start+n]...)
		axes = append(axes, s.RingAxes[start:start+n]...)
	} else {
		times = append(times, s.RingTimes[start:s.Len]...)
		times = append(times, s.RingTimes[0:start+n-s.Len]...)
		axes = append(axes, s.RingAxes[start:s.Len]...)
		axes = append(axes, s.RingAxes[0:start+n-s.Len]...)
	}

	return times, axes
}

type LayersConfig []LayerConfig

type Stat struct {
	mutex    sync.RWMutex
	layers   []*layerStat
	strategy matrix.Strategy
}

func NewStat(conf LayersConfig, strategy matrix.Strategy) *Stat {
	layers := make([]*layerStat, len(conf))
	// Fixme: Remove the test startTime.
	// startTime := time.Now()
	startTime := fileNextTime
	for i, c := range conf {
		layers[i] = newLayerStat(c, strategy, startTime)
		if i > 0 {
			layers[i-1].Next = layers[i]
		}
	}
	return &Stat{
		layers:   layers,
		strategy: strategy,
	}
}

// Append appends the all region information to statistics.
func (s *Stat) Append(regions []*core.RegionInfo, endTime time.Time) {
	if len(regions) == 0 {
		return
	}
	axis := toAxis(regions)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.layers[0].Append(axis, endTime)
}

func (s *Stat) Range(startTime, endTime time.Time) (times []time.Time, axes []matrix.Axis) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	times, axes = s.layers[0].Range(startTime, endTime, times, axes)
	return
}

func (s *Stat) RangePlane(startTime, endTime time.Time, startKey, endKey string, tags ...statTag) matrix.Plane {
	times, axes := s.Range(startTime, endTime)
	if len(times) <= 1 {
		return matrix.CreateEmptyPlane(startTime, endTime, startKey, endKey, len(tags))
	}
	for i, axis := range axes {
		axis = axis.Range(startKey, endKey)
		// Fixme: Remove the test Focus.
		tempMaxRow := 4 * maxDisplayY
		axis = axis.Focus(s.strategy, 1, len(axis.Keys)/tempMaxRow, tempMaxRow)
		axes[i] = filter(&axis, tags...)
	}
	return matrix.CreatePlane(times, axes)
}
