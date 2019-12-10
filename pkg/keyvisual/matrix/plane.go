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
	"time"
)

type Plane struct {
	// len(Times) == len(Axes) + 1
	Times []time.Time
	Axes  []Axis
}

func CreatePlane(times []time.Time, axes []Axis) Plane {
	if len(times) <= 1 {
		panic("Times length must be greater than 1")
	}
	return Plane{
		Times: times,
		Axes:  axes,
	}
}

func CreateEmptyPlane(startTime, endTime time.Time, startKey, endKey string, valuesListLen int) Plane {
	return CreatePlane([]time.Time{startTime, endTime}, []Axis{CreateEmptyAxis(startKey, endKey, valuesListLen)})
}

func (plane *Plane) Compact(strategy Strategy) Axis {
	chunks := make([]chunk, len(plane.Axes))
	for i, axis := range plane.Axes {
		chunks[i] = createChunk(axis.Keys, axis.ValuesList[0])
	}
	compactChunk := compact(strategy, chunks)
	valuesListLen := len(plane.Axes[0].ValuesList)
	valuesList := make([][]uint64, valuesListLen)
	valuesList[0] = compactChunk.Values
	for j := 1; j < valuesListLen; j++ {
		compactChunk.SetZeroValues()
		for i, axis := range plane.Axes {
			chunks[i].SetValues(axis.ValuesList[j])
			strategy.SplitAdd(compactChunk, chunks[i], i)
		}
		valuesList[j] = compactChunk.Values
	}
	strategy.End()
	return CreateAxis(compactChunk.Keys, valuesList)
}

func (plane *Plane) Pixel(strategy Strategy, target int) Matrix {
	axesLen := len(plane.Axes)
	chunks := make([]chunk, axesLen)
	for i, axis := range plane.Axes {
		chunks[i] = createChunk(axis.Keys, axis.ValuesList[0])
	}
	compactChunk := compact(strategy, chunks)
	baseKeys := compactChunk.Divide(strategy, target)
	valuesListLen := len(plane.Axes[0].ValuesList)
	matrix := createMatrix(strategy, plane.Times, baseKeys, valuesListLen)
	for j := 0; j < valuesListLen; j++ {
		matrix.Data[j] = make([][]uint64, axesLen)
		for i, axis := range plane.Axes {
			compactChunk.Clear()
			chunks[i].SetValues(axis.ValuesList[j])
			strategy.SplitTo(compactChunk, chunks[i], i)
			matrix.Data[j][i] = compactChunk.Reduce(baseKeys).Values
		}
	}
	strategy.End()
	return matrix
}

func compact(strategy Strategy, chunks []chunk) chunk {
	// get compact chunk keys
	keySet := make(map[string]struct{})
	unlimitedEnd := false
	for _, c := range chunks {
		end := len(c.Keys) - 1
		endKey := c.Keys[end]
		if endKey == "" {
			unlimitedEnd = true
		} else {
			keySet[endKey] = struct{}{}
		}
		for _, key := range c.Keys[:end] {
			keySet[key] = struct{}{}
		}
	}
	compactChunk := createZeroChunk(MakeKeys(keySet, unlimitedEnd))
	strategy.Start(chunks, compactChunk.Keys)
	for i, c := range chunks {
		strategy.SplitAdd(compactChunk, c, i)
	}
	return compactChunk
}
