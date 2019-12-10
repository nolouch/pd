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

// LabelKey labels the key.
type LabelKey struct {
	Key    string   `json:"key"`
	Labels []string `json:"labels"`
}

type Matrix struct {
	DataMap  map[string][][]uint64 `json:"data"`
	KeyAxis  []LabelKey            `json:"keyAxis"`
	TimeAxis []int64               `json:"timeAxis"`
}

func createMatrix(strategy Strategy, times []time.Time, keys []string, valuesListLen int) Matrix {
	dataMap := make(map[string][][]uint64, valuesListLen)
	// collect label keys
	keyAxis := make([]LabelKey, len(keys))
	for i, key := range keys {
		keyAxis[i] = strategy.Label(key)
	}
	// collect unix times
	timeAxis := make([]int64, len(times))
	for i, t := range times {
		timeAxis[i] = t.Unix()
	}
	return Matrix{
		DataMap:  dataMap,
		KeyAxis:  keyAxis,
		TimeAxis: timeAxis,
	}
}
