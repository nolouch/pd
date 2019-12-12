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
	"math"
	"sort"
)

type distanceHelper struct {
	Scale [][]float64
}

type distanceStrategy struct {
	LabelStrategy
	SplitRatio float64
	SplitLevel int
}

func DistanceStrategy(label LabelStrategy, ratio float64, level int) Strategy {
	return &distanceStrategy{
		SplitRatio:    1.0 / ratio,
		SplitLevel:    level,
		LabelStrategy: label,
	}
}

func (s *distanceStrategy) GenerateHelper(chunks []chunk, compactKeys []string) interface{} {
	axesLen := len(chunks)
	keysLen := len(compactKeys)
	virtualColumn := make([]int, keysLen)
	MemsetInt(virtualColumn, axesLen)
	dis := make([][]int, axesLen)
	scale := make([][]float64, axesLen)

	for i := 0; i < axesLen; i++ {
		dis[i] = make([]int, keysLen)
	}
	// left dis
	updateLeftDis(dis[0], virtualColumn, chunks[0].Keys, compactKeys)
	for i := 1; i < axesLen; i++ {
		updateLeftDis(dis[i], dis[i-1], chunks[i].Keys, compactKeys)
	}
	// right dis
	end := axesLen - 1
	updateRightDis(dis[end], virtualColumn, chunks[end].Keys, compactKeys)
	for i := end - 1; i >= 0; i-- {
		updateRightDis(dis[i], dis[i+1], chunks[i].Keys, compactKeys)
	}
	// key dis -> bucket dis
	for i := 0; i < axesLen; i++ {
		dis[i] = toBucketDis(dis[i])
	}
	// bucket dis -> bucket scale
	var tempDis []int
	tempMap := make(map[int]float64)
	for i := 0; i < axesLen; i++ {
		scale[i], tempDis = s.GenerateScale(dis[i], chunks[i].Keys, compactKeys, tempDis, tempMap)
	}
	return distanceHelper{Scale: scale}
}

func (s *distanceStrategy) SplitTo(dst, src chunk, axesIndex int, helper interface{}) {
	dstKeys := dst.Keys
	dstValues := dst.Values
	srcKeys := src.Keys
	srcValues := src.Values
	CheckPartOf(dstKeys, srcKeys)

	if len(dstKeys) == len(srcKeys) {
		copy(dstValues, srcValues)
		return
	}

	start := 0
	for startKey := srcKeys[0]; dstKeys[start] != startKey; start++ {
	}
	end := start + 1
	scale := helper.(distanceHelper).Scale
	for i, key := range srcKeys[1:] {
		for dstKeys[end] != key {
			end++
		}
		value := srcValues[i]
		for ; start < end; start++ {
			dstValues[start] = uint64(float64(value) * scale[axesIndex][start])
		}
		end++
	}
}

func (s *distanceStrategy) SplitAdd(dst, src chunk, axesIndex int, helper interface{}) {
	dstKeys := dst.Keys
	dstValues := dst.Values
	srcKeys := src.Keys
	srcValues := src.Values
	CheckPartOf(dstKeys, srcKeys)

	if len(dstKeys) == len(srcKeys) {
		for i, v := range srcValues {
			dstValues[i] += v
		}
		return
	}

	start := 0
	for startKey := srcKeys[0]; dstKeys[start] != startKey; start++ {
	}
	end := start + 1
	scale := helper.(distanceHelper).Scale
	for i, key := range srcKeys[1:] {
		for dstKeys[end] != key {
			end++
		}
		value := srcValues[i]
		for ; start < end; start++ {
			dstValues[start] += uint64(float64(value) * scale[axesIndex][start])
		}
		end++
	}
}

func (s *distanceStrategy) GenerateScale(dis []int, keys, compactKeys []string, tempDis []int, tempMap map[int]float64) ([]float64, []int) {
	scale := make([]float64, len(dis))
	start := 0
	for startKey := keys[0]; compactKeys[start] != startKey; start++ {
	}
	end := start + 1
	for _, key := range keys[1:] {
		for compactKeys[end] != key {
			end++
		}
		if start+1 == end {
			scale[start] = 1.0
			start++
		} else {
			// copy tempDis and calculate the top n levels
			tempDis = append(tempDis[:0], dis[start:end]...)
			tempLen := len(tempDis)
			sort.Ints(tempDis)
			level := 0
			tempMap[tempDis[0]] = 1.0
			tempValue := 1.0
			tempSum := 1.0
			for i := 1; i < tempLen; i++ {
				d := tempDis[i]
				if d != tempDis[i-1] {
					level++
					if level == s.SplitLevel {
						tempMap[d] = 0
					} else {
						tempValue = math.Pow(s.SplitRatio, float64(level))
						tempMap[d] = tempValue
					}
				}
				tempSum += tempValue
			}
			// calculate scale
			for ; start < end; start++ {
				scale[start] = tempMap[dis[start]] / tempSum
			}
		}
		end++
	}
	return scale, tempDis
}

func updateLeftDis(dis, leftDis []int, keys, compactKeys []string) {
	CheckPartOf(compactKeys, keys)
	j := 0
	for i := range dis {
		if compactKeys[i] == keys[j] {
			dis[i] = 0
			j++
		} else {
			dis[i] = leftDis[i] + 1
		}
	}
}

func updateRightDis(dis, rightDis []int, keys, compactKeys []string) {
	j := 0
	for i := range dis {
		if compactKeys[i] == keys[j] {
			dis[i] = 0
			j++
		} else {
			dis[i] = Min(dis[i], rightDis[i]+1)
		}
	}
}

func toBucketDis(dis []int) []int {
	for i := len(dis) - 1; i > 0; i-- {
		dis[i] = Max(dis[i], dis[i-1])
	}
	return dis[1:]
}
