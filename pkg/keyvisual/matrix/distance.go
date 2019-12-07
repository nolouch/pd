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

type distanceStrategy struct {
	LabelStrategy
	Scale [][]float64
}

func DistanceStrategy(label LabelStrategy) Strategy {
	return &distanceStrategy{
		LabelStrategy: label,
	}
}

func (s *distanceStrategy) Start(chunks []chunk, compactKeys []string) {
	// TODO: Calculate Scale
}

func (s *distanceStrategy) End() {
	s.Scale = nil
}

func (s *distanceStrategy) SplitTo(dst, src chunk, axesIndex int) {
	dstKeys := dst.Keys
	dstValues := dst.Values
	start := 0
	end := 1
	srcValues := src.Values
	CheckPartOf(dstKeys, src.Keys)
	scale := s.Scale
	for i, key := range src.Keys[1:] {
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

func (s *distanceStrategy) SplitAdd(dst, src chunk, axesIndex int) {
	dstKeys := dst.Keys
	dstValues := dst.Values
	start := 0
	end := 1
	srcValues := src.Values
	CheckPartOf(dstKeys, src.Keys)
	scale := s.Scale
	for i, key := range src.Keys[1:] {
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
