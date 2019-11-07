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

package matrix

import (
	"sort"
	"time"
)

// DiscreteAxis represents one key axis at one time.
type DiscreteAxis struct {
	StartKey string // StartKey for the first line
	Lines    []*Line
	EndTime  time.Time // the time of the key axis
}

type DiscreteKeys []string

func (axis *DiscreteAxis) Clone() *DiscreteAxis {
	newAxis := &DiscreteAxis{
		StartKey: axis.StartKey,
		EndTime:  axis.EndTime,
	}
	for i := 0; i < len(axis.Lines); i++ {
		line := &Line{
			EndKey: axis.Lines[i].EndKey,
			Value:  axis.Lines[i].Value.Clone(),
		}
		newAxis.Lines = append(newAxis.Lines, line)
	}
	return newAxis
}

// Effect evaluete the count can be denoised to.
func (axis *DiscreteAxis) Effect(threshold uint64) uint {
	var num uint = 0
	// flag that the last line is lower than the threshold
	isLastLess := false
	var lastValue int64 = -1 // last line value
	for _, line := range axis.Lines {
		if line.Less(threshold) {
			isLastLess = true
		} else {
			if lastValue == -1 || line.GetThreshold() != uint64(lastValue) {
				num++
			}
			if isLastLess {
				isLastLess = false
				num++
			}
		}
		lastValue = int64(line.GetThreshold())
	}
	if isLastLess {
		num++
	}
	return num
}

// DeNoise merge line segments below the amount of information with a specified threshold.
// adjacent lines and the value lower than threshold can be merge to one line.
// adjacent lines with same value can be merge to one line.
func (axis *DiscreteAxis) DeNoise(threshold uint64) {
	newAxis := make([]*Line, 0)
	// flag that the last line is lower than the threshold
	isLastLess := false
	var lastIndex int64 = -1 //last line value
	for _, line := range axis.Lines {
		if line.Less(threshold) {
			if isLastLess {
				// merge the line with last less threshold.
				newAxis[len(newAxis)-1].Value.Merge(line.Value)
				newAxis[len(newAxis)-1].EndKey = line.EndKey
			} else {
				isLastLess = true
				newAxis = append(newAxis, line)
			}
		} else {
			isLastLess = false
			if lastIndex == -1 || !line.Value.Equal(axis.Lines[lastIndex].Value) {
				newAxis = append(newAxis, line)
			} else {
				newAxis[len(newAxis)-1].Value.Merge(line.Value)
				newAxis[len(newAxis)-1].EndKey = line.EndKey
			}
		}
		lastIndex++
	}
	axis.Lines = newAxis
}

// ReSample rebuild the key axis by the specified key.
func (axis *DiscreteAxis) ReSample(dst *DiscreteAxis) {
	srcKeys := axis.GetDiscreteKeys()
	dstKeys := dst.GetDiscreteKeys()
	lengthSrc := len(srcKeys)
	lengthDst := len(dstKeys)
	startIndex := 0
	endIndex := 0
	for i := 1; i < lengthSrc; i++ {
		for j := endIndex; j < lengthDst; j++ {
			if dstKeys[j] == srcKeys[i-1] {
				startIndex = j
			}
			if dstKeys[j] == srcKeys[i] {
				endIndex = j
				break
			}
		}
		count := endIndex - startIndex
		if count == 0 {
			continue
		}
		newAxis := axis.Lines[i-1].Split(count)
		for j := startIndex; j < endIndex; j++ {
			dst.Lines[j].Merge(newAxis)
		}
	}
}

// DeProjection project the src key-axis to dest key-axis.
// the values dest key-axis should be 0.
func (axis *DiscreteAxis) DeProjection(dst *DiscreteAxis) {
	lengthSrc := len(axis.Lines)
	lengthDst := len(dst.Lines)

	// SrcI and DstI, the first intersecting with start lines index.
	var DstI int
	var SrcI int
	if axis.StartKey < dst.StartKey {
		DstI = 0
		SrcI = sort.Search(lengthSrc, func(i int) bool {
			return axis.Lines[i].EndKey >= dst.StartKey
		})
	} else {
		DstI = sort.Search(lengthDst, func(i int) bool {
			return axis.StartKey < dst.Lines[i].EndKey
		})
	}

	startIndex := DstI
	var endIndex int
	for DstI < lengthDst && SrcI < lengthSrc {
		// find the src to dest asis' projection.
		if axis.Lines[SrcI].EndKey <= dst.Lines[DstI].EndKey {
			endIndex = DstI
			// find the index range, do merge.
			for i := startIndex; i <= endIndex; i++ {
				dst.Lines[i].Value.Merge(axis.Lines[SrcI].Value)
			}
			if axis.Lines[SrcI].EndKey == dst.Lines[DstI].EndKey {
				DstI++
				// Fixme: do not exsist multiple equal keys.
				for DstI < lengthDst && dst.Lines[DstI].EndKey == axis.Lines[SrcI].EndKey {
					dst.Lines[DstI].Value.Merge(axis.Lines[SrcI].Value)
					DstI++
				}
				startIndex = DstI
			} else {
				startIndex = endIndex
			}
			SrcI++
		} else {
			DstI++
		}
	}
}

// GetDiscreteKeys returns the keys above this axis.
func (axis *DiscreteAxis) GetDiscreteKeys() DiscreteKeys {
	discreteKeys := make(DiscreteKeys, 0)
	discreteKeys = append(discreteKeys, axis.StartKey)
	for _, key := range axis.Lines {
		discreteKeys = append(discreteKeys, key.EndKey)
	}
	return discreteKeys
}

// Range return a key-axis with specified range.
func (axis *DiscreteAxis) Range(startKey string, endKey string) *DiscreteAxis {
	newAxis := &DiscreteAxis{
		StartKey: "",
		EndTime:  axis.EndTime,
	}
	if endKey <= axis.StartKey {
		return newAxis
	}
	size := len(axis.Lines)
	startIndex := sort.Search(size, func(i int) bool {
		return axis.Lines[i].EndKey > startKey
	})
	if startIndex == size {
		return newAxis
	}

	endIndex := sort.Search(size, func(i int) bool {
		return axis.Lines[i].EndKey >= endKey
	})
	if endIndex != size {
		endIndex++
	}

	if startIndex == 0 {
		newAxis.StartKey = axis.StartKey
	} else {
		newAxis.StartKey = axis.Lines[startIndex-1].EndKey
	}
	newAxis.Lines = make([]*Line, 0, endIndex-startIndex)
	for i := startIndex; i < endIndex; i++ {
		line := &Line{
			axis.Lines[i].EndKey,
			axis.Lines[i].Value.Clone(),
		}
		newAxis.Lines = append(newAxis.Lines, line)
	}
	return newAxis
}
