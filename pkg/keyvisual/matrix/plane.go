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

	"github.com/pingcap/log"
	"go.uber.org/zap"
	"sort"
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
	Keys  []LabelKey      `json:"keyAxis"`  // key-axes
	Times []int64         `json:"timeAxis"` // time-axes
}

// GetDiscreteTimes gets all times.
func (plane *DiscretePlane) GetDiscreteTimes() DiscreteTimes {
	discreteTimes := make(DiscreteTimes, 0, len(plane.Axes)+1)
	discreteTimes = append(discreteTimes, plane.StartTime)
	for _, axis := range plane.Axes {
		discreteTimes = append(discreteTimes, axis.EndTime)
	}
	return discreteTimes
}

// Compact compacts multiple key axes into one axis.
func (plane *DiscretePlane) Compact() (axis *DiscreteAxis, startTime time.Time) {
	startTime = plane.StartTime
	axis = new(DiscreteAxis)
	length := len(plane.Axes)
	if length == 0 {
		return axis, startTime
	}
	axis.EndTime = plane.Axes[length-1].EndTime
	// Figure out all keys.
	isInside := make(map[string]struct{})
	allKeys := make([]string, 0, len(plane.Axes[0].Lines))
	for _, d := range plane.Axes {
		if len(d.Lines) == 0 {
			continue
		}
		for i, k := range d.GetDiscreteKeys() {
			if _, ok := isInside[k]; !ok {
				isInside[k] = struct{}{}
				allKeys = append(allKeys, k)
			}
			if i > 0 && k == "" {
				isInside["EmptyEndKey_xmaigic"] = struct{}{}
			}
		}
	}
	sort.Strings(allKeys)
	log.Info("allKeys", zap.Int("key-len", len(allKeys)))

	// Fixme: default value.
	var defaultValue Value
	for _, axis := range plane.Axes {
		for _, Line := range axis.Lines {
			defaultValue = Line.Default()
			break
		}
		if defaultValue != nil {
			break
		}
	}

	if len(allKeys) == 0 {
		axis.StartKey = ""
	} else {
		axis.StartKey = allKeys[0]
	}
	if _, ok := isInside["EmptyEndKey_xmaigic"]; ok {
		allKeys = append(allKeys, "")
	}
	length = len(allKeys)
	if length > 0 {
		axis.Lines = make([]*Line, 0, length-1)
	}
	for i := 1; i < length; i++ {
		newLine := &(Line{
			EndKey: allKeys[i],
			Value:  defaultValue.Default(),
		})
		axis.Lines = append(axis.Lines, newLine)
	}
	for _, ax := range plane.Axes {
		ax.ReSample(axis)
	}
	return axis, startTime
}

// Pixel generates the Matrix[n,m].
func (plane *DiscretePlane) Pixel(n int, m int, typ string) *Matrix {
	newPlane := DiscretePlane{
		StartTime: plane.StartTime,
	}
	if n == 0 || m == 0 {
		return nil
	}
	// time-axis, avg compress
	//if len(plane.Axes) >= n {
	//	// the first part use step1, then step2
	//	step2 := len(plane.Axes) / n
	//	step1 := step2 + 1
	//	n1 := len(plane.Axes) % n
	//	var index int
	//	var step int

	//	for i := 0; i < n; i++ {
	//		if i < n1 {
	//			step = step1
	//			index = i * step1
	//		} else {
	//			step = step2
	//			index = n1*step1 + (i-n1)*step2
	//		}
	//		// merge to one key-axis
	//		tempPlane := &DiscretePlane{}
	//		if i == 0 {
	//			tempPlane.StartTime = plane.StartTime
	//		} else {
	//			tempPlane.StartTime = plane.Axes[index-1].EndTime
	//		}
	//		tempPlane.Axes = make([]*DiscreteAxis, step)
	//		for i := 0; i < step; i++ {
	//			tempPlane.Axes[i] = plane.Axes[index+i].Clone()
	//		}
	//		axis, _ := tempPlane.Compact()

	//		// append to new plane
	//		newPlane.Axes = append(newPlane.Axes, axis)
	//	}
	//} else {
	//	newPlane.Axes = make([]*DiscreteAxis, len(plane.Axes))
	//	for i := 0; i < len(plane.Axes); i++ {
	//		newPlane.Axes[i] = plane.Axes[i].Clone()
	//	}
	//}

	// Do not compress the time-axis
	if len(plane.Axes) >= n {
		newPlane.Axes = make([]*DiscreteAxis, n)
		length := len(plane.Axes)
		for i := 0; i < n; i++ {
			j := length - n + i
			newPlane.Axes[i] = plane.Axes[j].Clone()
		}
	} else {
		newPlane.Axes = make([]*DiscreteAxis, len(plane.Axes))
		for i := 0; i < len(plane.Axes); i++ {
			newPlane.Axes[i] = plane.Axes[i].Clone()
		}
	}

	// try to get a base axis.
	// then all axes can do project to the base-axis
	axis, _ := newPlane.Compact()
	if len(axis.Lines) > m {
		log.Info("the keys is more than threshold",
			zap.Int("threshold", m),
			zap.Int("lines", len(axis.Lines)))
		// get all thresholds, then fine a threshold to compress
		thresholdSet := make(map[uint64]struct{}, len(axis.Lines))
		for _, line := range axis.Lines {
			thresholdSet[line.GetValue(typ)] = struct{}{}
		}
		thresholdSet[0] = struct{}{}
		thresholds := make([]uint64, 0, len(thresholdSet))
		for threshold := range thresholdSet {
			thresholds = append(thresholds, threshold)
		}
		sort.Slice(thresholds, func(i, j int) bool { return thresholds[i] < thresholds[j] })

		i := sort.Search(len(thresholds), func(i int) bool {
			return axis.Effect(thresholds[i], typ) <= uint(m)
		})

		// find a better threshold
		if i >= len(thresholds) {
			i = i - 1
		}
		threshold1 := thresholds[i]
		num1 := axis.Effect(threshold1, typ)
		//	for z := range thresholds {
		//		log.S().Info("eeffee:", z, axis.Effect(thresholds[z], typ), thresholds[z])
		//	}
		if i > 0 && num1 != uint(m) && num1 > uint(m) {
			threshold2 := thresholds[i-1]
			num2 := axis.Effect(threshold2, typ)
			if (int(num2) - m) < (m - int(num1)) {
				axis.DeNoise2(threshold2, m, typ)
			} else {
				axis.DeNoise2(threshold1, m, typ)
			}
		} else {
			axis.DeNoise2(threshold1, m, typ)
		}
	}
	//log.Info("axis", zap.Reflect("lines", axis.Lines))

	// reset the value then do projection.
	for i := 0; i < len(axis.Lines); i++ {
		axis.Lines[i].Reset()
	}

	log.Info("the new  keys number", zap.Int("keys-number", len(axis.Lines)))
	for i := 0; i < len(newPlane.Axes); i++ {
		axisClone := axis.Clone()
		newPlane.Axes[i].DeProjection(axisClone)
		axisClone.EndTime = newPlane.Axes[i].EndTime
		newPlane.Axes[i] = axisClone
	}

	// generates matrix
	discreteTimes := newPlane.GetDiscreteTimes()
	discreteKeys := axis.GetDiscreteKeys()
	timesLen := len(discreteTimes) - 1
	keysLen := len(discreteKeys) - 1
	matrix := &Matrix{
		Data:  make([][]interface{}, timesLen),
		Keys:  collectLabelKeys(discreteKeys),
		Times: collectUnixTimes(discreteTimes),
	}

	for i := 0; i < timesLen; i++ {
		matrix.Data[i] = make([]interface{}, keysLen)
		for j := 0; j < keysLen; j++ {
			matrix.Data[i][j] = newPlane.Axes[i].Lines[j].Value.GetValue(typ)
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
