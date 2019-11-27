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

func NewEmptyAxis(endTime time.Time, startKey, endKey string, zeroValue Value) *DiscreteAxis {
	return &DiscreteAxis{
		StartKey: startKey,
		Lines: []*Line{
			{
				EndKey: endKey,
				Value:  zeroValue.Clone(),
			},
		},
		EndTime: endTime,
	}
}

func NewZeroAxis(endTime time.Time, keySet map[string]struct{}, endUnlimited bool, zeroValue Value) *DiscreteAxis {
	keys := make([]string, len(keySet))
	i := 0
	for key := range keySet {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	axis := &DiscreteAxis{
		StartKey: keys[0],
		Lines:    make([]*Line, len(keySet)-1),
		EndTime:  endTime,
	}

	for i, key := range keys[1:] {
		axis.Lines[i] = &Line{
			EndKey: key,
			Value:  zeroValue.Clone(),
		}
	}

	if endUnlimited {
		axis.Lines = append(axis.Lines, &Line{
			EndKey: "",
			Value:  zeroValue.Clone(),
		})
	}
	return axis
}

// If zeroValue is nil, clone line.Value. Otherwise clone zeroValue
func (axis *DiscreteAxis) Clone(zeroValue Value) *DiscreteAxis {
	newAxis := &DiscreteAxis{
		StartKey: axis.StartKey,
		EndTime:  axis.EndTime,
		Lines:    make([]*Line, len(axis.Lines)),
	}
	for i, line := range axis.Lines {
		if zeroValue == nil {
			newAxis.Lines[i] = &Line{
				EndKey: line.EndKey,
				Value:  line.Value.Clone(),
			}
		} else {
			newAxis.Lines[i] = &Line{
				EndKey: line.EndKey,
				Value:  zeroValue.Clone(),
			}
		}
	}
	return newAxis
}

// GetDiscreteKeys returns the keys above this axis.
func (axis *DiscreteAxis) GetDiscreteKeys() DiscreteKeys {
	discreteKeys := make(DiscreteKeys, len(axis.Lines)+1)
	discreteKeys[0] = axis.StartKey
	for i, line := range axis.Lines {
		discreteKeys[i+1] = line.EndKey
	}
	return discreteKeys
}

// The rows from startIndex to endIndex are merged into startIndex and returned.
func (axis *DiscreteAxis) compact(startIndex, endIndex int) *Line {
	if startIndex >= endIndex {
		panic("CompactUnit's endIndex should be greater than startIndex.")
	} else if startIndex+1 == endIndex {
		return axis.Lines[startIndex]
	} else {
		line := axis.Lines[startIndex]
		for i := startIndex + 1; i < endIndex; i++ {
			line.Merge(axis.Lines[i].Value)
		}
		line.EndKey = axis.Lines[endIndex-1].EndKey
		return line
	}
}

// ReSample rebuild the key axis by the specified key.
func (axis *DiscreteAxis) SplitReSample(dst *DiscreteAxis) {
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
		splitValue := axis.Lines[i-1].Split(endIndex - startIndex)
		for j := startIndex; j < endIndex; j++ {
			dst.Lines[j].Merge(splitValue)
		}
	}
}

func (axis *DiscreteAxis) MergeReSample(keys DiscreteKeys) {
	if axis.StartKey != keys[0] {
		panic("DeNoiseByKeys requires the same startKey.")
	}
	newLines := make([]*Line, len(keys)-1)
	endKey := 1
	startIndex := 0
	for i, line := range axis.Lines {
		if line.EndKey == keys[endKey] {
			newLines[endKey-1] = axis.compact(startIndex, i+1)
			endKey++
			startIndex = i + 1
		}
	}
	axis.Lines = newLines
}

// DeNoise merge line segments below the amount of information with a specified threshold.
// adjacent lines and the value lower than threshold can be merge to one line.
// adjacent lines with same value can be merge to one line.
// The return value is the number of rows after merge.
// If `ratio` is 0, it means that only the return value is calculated and no operation is performed.
func (axis *DiscreteAxis) DeNoise(tag ValueTag, threshold uint64, ratio int, target int) int {
	count := 0
	startIndex := 0
	bucketRows := 0
	var bucketSum uint64 = 0

	var newLines []*Line
	if ratio > 0 {
		newLines = make([]*Line, 0, target)
	}

	generateBucket := func(endIndex int) {
		if startIndex == endIndex {
			return
		}
		count++
		if ratio > 0 {
			newLines = append(newLines, axis.compact(startIndex, endIndex))
		}
		startIndex = endIndex
		bucketRows = 0
		bucketSum = 0
	}

	bucketAdd := func(stepStartIndex, stepEndIndex int) {
		stepValue := axis.Lines[stepStartIndex].GetValue(tag) * uint64(stepEndIndex-stepStartIndex)
		if stepValue >= threshold {
			generateBucket(stepStartIndex)
		}
		bucketRows++
		bucketSum += stepValue
		if (ratio > 0 && bucketRows == ratio) || bucketSum >= threshold {
			generateBucket(stepEndIndex)
		}
	}

	equalIndex := 0
	for i, line := range axis.Lines {
		if i > 0 && (ratio == 0 || line.GetValue(tag) < threshold || !line.Equal(axis.Lines[i-1].Value)) {
			bucketAdd(equalIndex, i)
			equalIndex = i
		}
	}
	bucketAdd(equalIndex, len(axis.Lines))
	generateBucket(len(axis.Lines))

	if ratio > 0 {
		axis.Lines = newLines
	}
	return count
}

func (axis *DiscreteAxis) Divide(maxRow int, tag ValueTag, zeroValue Value) DiscreteKeys {
	if maxRow >= len(axis.Lines) {
		return axis.GetDiscreteKeys()
	}
	// get upperThreshold
	line0 := &Line{
		EndKey: axis.Lines[0].EndKey,
		Value:  axis.Lines[0].Clone(),
	}
	compactLine := axis.compact(0, len(axis.Lines))
	upperThreshold := compactLine.GetValue(tag) + 1
	axis.Lines[0] = line0
	// search threshold
	var lowerThreshold uint64 = 1
	target := maxRow * 2 / 3
	for lowerThreshold < upperThreshold {
		mid := (lowerThreshold + upperThreshold) >> 1
		if axis.DeNoise(tag, mid, 0, 0) > target {
			lowerThreshold = mid + 1
		} else {
			upperThreshold = mid
		}
	}
	threshold := lowerThreshold
	effectRow := axis.DeNoise(tag, lowerThreshold, 0, 0)
	ratio := len(axis.Lines)/(maxRow-effectRow) + 1
	newAxis := axis.Clone(nil)
	newAxis.DeNoise(tag, threshold, ratio, maxRow)
	return newAxis.GetDiscreteKeys()
}

// Range return a key-axis with specified range.
func (axis *DiscreteAxis) Range(startKey string, endKey string, zeroValue Value) *DiscreteAxis {
	if endKey != "" && endKey <= axis.StartKey {
		return NewEmptyAxis(axis.EndTime, startKey, endKey, zeroValue)
	}
	size := len(axis.Lines)
	startIndex := sort.Search(size, func(i int) bool {
		return axis.Lines[i].EndKey > startKey
	})
	if startIndex == size {
		return NewEmptyAxis(axis.EndTime, startKey, endKey, zeroValue)
	}

	var endIndex int
	if endKey == "" {
		endIndex = size
	} else {
		endIndex = sort.Search(size, func(i int) bool {
			return axis.Lines[i].EndKey >= endKey
		})
	}

	if endIndex != size {
		endIndex++
	}

	newAxis := new(DiscreteAxis)
	newAxis.EndTime = axis.EndTime
	if startIndex == 0 {
		newAxis.StartKey = axis.StartKey
	} else {
		newAxis.StartKey = axis.Lines[startIndex-1].EndKey
	}
	newAxis.Lines = axis.Lines[startIndex:endIndex]
	return newAxis
}
