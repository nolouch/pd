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

type averageStrategy struct {
	LabelStrategy
}

func AverageStrategy(label LabelStrategy) Strategy {
	return averageStrategy{
		LabelStrategy: label,
	}
}

func (_ averageStrategy) Start(_ []chunk, _ []string) {}

func (_ averageStrategy) End() {}

func (_ averageStrategy) SplitTo(dst, src chunk, _ int) {
	dstKeys := dst.Keys
	dstValues := dst.Values
	start := 0
	end := 1
	srcValues := src.Values
	CheckPartOf(dstKeys, src.Keys)
	for i, key := range src.Keys[1:] {
		for dstKeys[end] != key {
			end++
		}
		value := srcValues[i] / uint64(end-start)
		for ; start < end; start++ {
			dstValues[start] = value
		}
		end++
	}
}

func (_ averageStrategy) SplitAdd(dst, src chunk, _ int) {
	dstKeys := dst.Keys
	dstValues := dst.Values
	start := 0
	end := 1
	srcValues := src.Values
	CheckPartOf(dstKeys, src.Keys)
	for i, key := range src.Keys[1:] {
		for dstKeys[end] != key {
			end++
		}
		value := srcValues[i] / uint64(end-start)
		for ; start < end; start++ {
			dstValues[start] += value
		}
		end++
	}
}
