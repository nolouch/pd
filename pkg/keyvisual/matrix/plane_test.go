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
	"bytes"
	"fmt"
	"testing"
	"time"
)

func buildTime(subtract int) time.Time {
	dur, _ := time.ParseDuration(fmt.Sprintf("-%dm", subtract))
	return time.Now().Add(dur)
}

func BuildDiscretePlane(times []int, keys [][]string, values [][]uint64) *DiscretePlane {
	plane := &DiscretePlane{
		StartTime: buildTime(times[0]),
		Axes:      make([]*DiscreteAxis, len(times)-1),
	}
	for i := 0; i < len(keys); i++ {
		plane.Axes[i] = BuildDiscreteAxis(keys[i][0], keys[i][1:], values[i], buildTime(times[i+1]))
	}
	return plane
}

func (mx *Matrix) String() string {
	var buf bytes.Buffer
	buf.WriteString("Keys:")
	for _, key := range mx.Keys {
		buf.WriteString(" ")
		buf.WriteString(key.Key)
	}
	buf.WriteString("\nTimes:")
	for _, ts := range mx.Times {
		buf.WriteString(" ")
		buf.WriteString(fmt.Sprint(ts))
	}
	buf.WriteString("\nData:\n")
	for _, row := range mx.Data {
		for _, v := range row {
			buf.WriteString(" ")
			buf.WriteString(fmt.Sprint(v))
		}
		buf.WriteString("\n")
	}
	return buf.String()
}

func TestDiscretePlane_Compact(t *testing.T) {
	times := []int{20, 15, 10, 5, 0}
	keys := [][]string{
		{"a", "z"},
		{"", "b", "f", "h", "i"},
		{"a", "d", "i", "n", "q"},
		{"", "e", "i", "k", "n"},
	}
	values := [][]uint64{
		{0},
		{1, 5, 4, 10},
		{5, 0, 1, 6},
		{0, 3, 7, 9},
	}
	plane := BuildDiscretePlane(times, keys, values)
	dstAxis, _ := plane.Compact(zeroValueUint64)

	endTime := plane.Axes[len(plane.Axes)-1].EndTime
	expectStartKey := ""
	expectKeyList := []string{"a", "b", "d", "e", "f", "h", "i", "k", "n", "q", "z"}
	expectValueList := []uint64{0, 2, 3, 1, 2, 5, 11, 7, 9, 6, 0}
	expectAxis := BuildDiscreteAxis(expectStartKey, expectKeyList, expectValueList, endTime)
	AssertEq(t, dstAxis, expectAxis)
}

func TestDiscretePlane_Pixel(t *testing.T) {
	matrixEq := func(dst *Matrix, expect [][]uint64) bool {
		if len(dst.Data) != len(expect) {
			return false
		}
		for i, row := range dst.Data {
			if len(row) != len(expect[i]) {
				return false
			}
			for j, value := range row {
				if value.(uint64) != expect[i][j] {
					return false
				}
			}
		}
		return true
	}

	times := []int{20, 15, 10, 5, 0}
	keys := [][]string{
		{"b", "c", "e", "l", "m", "o"},
		{"", "b", "f", "h", "i", "k"},
		{"a", "d", "i", "n", "q", "r"},
		{"", "e", "i", "k", "n", "o"},
	}
	values := [][]uint64{
		{3, 0, 6, 0, 9},
		{1, 5, 4, 10, 7},
		{5, 0, 1, 6, 4},
		{0, 3, 7, 9, 5},
	}
	plane := BuildDiscretePlane(times, keys, values)
	dstMatrix := plane.Pixel(12, INTEGRATION, zeroValueUint64)
	expectMatrix := [][]uint64{
		{3, 2, 1, 1, 1, 4, 4, 0},
		{2, 6, 10, 7, 0, 0, 0, 0},
		{3, 0, 0, 0, 0, 0, 3, 7},
		{0, 2, 1, 7, 6, 3, 5, 0},
	}
	if !matrixEq(dstMatrix, expectMatrix) {
		t.Fatalf("expect\n%v\nbut got\n%v", expectMatrix, dstMatrix)
	}
}
