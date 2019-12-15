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

type averageHelper struct {
}

type averageStrategy struct {
	LabelStrategy
}

func AverageStrategy(label LabelStrategy) Strategy {
	return averageStrategy{
		LabelStrategy: label,
	}
}

func (_ averageStrategy) GenerateHelper(_ []chunk, _ []string) interface{} {
	return averageHelper{}
}

func (_ averageStrategy) SplitTo(dst, src chunk, _ int, _ interface{}) {
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
	for i, key := range srcKeys[1:] {
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

func (_ averageStrategy) SplitAdd(dst, src chunk, _ int, _ interface{}) {
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
	for i, key := range srcKeys[1:] {
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
