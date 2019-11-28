// Copyright 201 PingCAP, Inc.
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

package matrix

import (
	"encoding/hex"
	"time"
)

type DiscretePlane struct {
	StartTime time.Time
	Axes      []*DiscreteAxis
}

type DiscreteTimes []time.Time

// LabelKey labels the key.
type LabelKey struct {
	Key    string   `json:"key"`
	Labels []string `json:"labels"`
}

type Matrix struct {
	Data  [][]interface{} `json:"values"`   // data of the matrix
	Keys  []LabelKey      `json:"keyAxis"`  // key-axis
	Times []int64         `json:"timeAxis"` // time-axis
}

func NewEmptyPlane(startTime, endTime time.Time, startKey, endKey string, zeroValue Value) *DiscretePlane {
	return &DiscretePlane{
		StartTime: startTime,
		Axes: []*DiscreteAxis{
			NewEmptyAxis(endTime, startKey, endKey, zeroValue),
		},
	}
}

// GetDiscreteTimes gets all times.
func (plane *DiscretePlane) GetDiscreteTimes() DiscreteTimes {
	discreteTimes := make(DiscreteTimes, len(plane.Axes)+1)
	discreteTimes[0] = plane.StartTime
	for i, axis := range plane.Axes {
		discreteTimes[i+1] = axis.EndTime
	}
	return discreteTimes
}

// Compact compacts multiple key axes into one axis.
func (plane *DiscretePlane) Compact(zeroValue Value) (*DiscreteAxis, time.Time) {
	keySet := make(map[string]struct{})
	endUnlimited := false
	for _, axis := range plane.Axes {
		keys := axis.GetDiscreteKeys()
		for _, key := range keys {
			keySet[key] = struct{}{}
		}
		if keys[len(keys)-1] == "" {
			endUnlimited = true
		}
	}
	newAxis := NewZeroAxis(plane.Axes[len(plane.Axes)-1].EndTime, keySet, endUnlimited, zeroValue)
	for _, axis := range plane.Axes {
		axis.SplitReSample(newAxis)
	}
	return newAxis, plane.StartTime
}

func (plane *DiscretePlane) Pixel(maxRow int, tag ValueTag, zeroValue Value) *Matrix {
	// get splitAxis and merged DiscreteKeys
	splitAxis, _ := plane.Compact(zeroValue)
	keys := splitAxis.Divide(maxRow, tag, zeroValue)
	// generate Matrix
	keysLen := len(keys) - 1
	times := plane.GetDiscreteTimes()
	timesLen := len(times) - 1
	matrix := &Matrix{
		Data:  make([][]interface{}, timesLen),
		Keys:  collectLabelKeys(keys),
		Times: collectUnixTimes(times),
	}
	for i := 0; i < timesLen; i++ {
		axis := splitAxis.Clone(zeroValue)
		plane.Axes[i].SplitReSample(axis)
		axis.MergeReSample(keys)
		matrix.Data[i] = make([]interface{}, keysLen)
		for j := 0; j < keysLen; j++ {
			matrix.Data[i][j] = axis.Lines[j].GetValue(tag)
		}
	}
	return matrix
}

func collectLabelKeys(keys DiscreteKeys) []LabelKey {
	newKeys := make([]LabelKey, len(keys))
	for i, key := range keys {
		// TODO: Parse from label
		newKeys[i] = LabelKey{
			Key:    hex.EncodeToString([]byte(key)),
			Labels: []string{},
		}
	}
	return newKeys
}

func collectUnixTimes(times DiscreteTimes) []int64 {
	res := make([]int64, len(times))
	for i, t := range times {
		res[i] = t.Unix()
	}
	return res
}
