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
	"fmt"
	"reflect"
	"testing"
	"time"
)

type valueUint64 struct {
	uint64
}

func max(a uint64, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func (v *valueUint64) Split(count int) Value {
	res := *v
	return &res
}
func (v *valueUint64) Merge(other Value) {
	v2 := other.(*valueUint64)
	v.uint64 = max(v.uint64, v2.uint64)
}

func (v *valueUint64) Less(threshold uint64) bool {
	return v.uint64 < threshold
}
func (v *valueUint64) GetThreshold() uint64 {
	return v.uint64
}

func (v *valueUint64) Clone() Value {
	clonevalueUint64 := *v
	return &clonevalueUint64
}

func (v *valueUint64) Reset() {
	*v = valueUint64{}
}

func (v *valueUint64) Default() Value {
	return new(valueUint64)
}

func (v *valueUint64) Equal(other Value) bool {
	another := other.(*valueUint64)
	return *v == *another
}

func SprintDiscreteAxis(axis *DiscreteAxis) string {
	str := fmt.Sprintf("StartKey: %v\n", axis.StartKey)
	for _, line := range axis.Lines {
		str += fmt.Sprintf("[%v, %v]", line.GetThreshold(), line.EndKey)
	}
	str += fmt.Sprintf("\nEndTime: %v\n", axis.EndTime)
	return str
}

func BuildDiscreteAxis(startKey string, keys []string, values []uint64, endTime time.Time) *DiscreteAxis {
	line := make([]*Line, len(values))
	for i := 0; i < len(values); i++ {
		line[i] = &Line{keys[i], &valueUint64{values[i]}}
	}
	return &DiscreteAxis{
		StartKey: startKey,
		Lines:    line,
		EndTime:  endTime,
	}
}

func TestClone(t *testing.T) {
	startKey := ""
	uint64List := []uint64{0, 0, 10, 2, 4, 3, 0, 7, 11, 2}
	endKeyList := []string{"a", "b", "d", "e", "h", "i", "k", "l", "t", "z"}
	endTime := time.Now()
	axis := BuildDiscreteAxis(startKey, endKeyList, uint64List, endTime)
	axisClone := axis.Clone()
	if !reflect.DeepEqual(axisClone, axis) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(axisClone), SprintDiscreteAxis(axis))
	}

	bigUint64 := uint64(100000)
	expectUint64 := axis.Lines[0].GetThreshold()
	axisClone.Lines[0].Merge(&valueUint64{bigUint64})
	if reflect.DeepEqual(axis, axisClone) {
		t.Fatalf("expect %v, but got %v", expectUint64, bigUint64)
	}
}

func TestEffect(t *testing.T) {
	startKey := ""
	uint64List := []uint64{0, 0, 10, 2, 3, 3, 0, 7, 11, 2}
	endKeyList := []string{"a", "b", "d", "e", "h", "i", "k", "l", "t", "z"}
	endTime := time.Now()
	axis := BuildDiscreteAxis(startKey, endKeyList, uint64List, endTime)

	max := 13
	num := make([]uint, max)

	expect := []uint{8, 8, 8, 8, 6, 6, 6, 6, 5, 5, 5, 3, 1}
	for i := 0; i < max; i++ {
		num[i] = axis.Effect(uint64(i))
	}

	if !reflect.DeepEqual(num, expect) {
		t.Fatalf("expect %v, but got %v", expect, num)
	}
}

func TestDeNoise(t *testing.T) {
	startKey := ""
	uint64List := []uint64{4, 0, 10, 2, 3, 3, 0, 7, 11, 2, 1}
	endKeyList := []string{"a", "b", "d", "e", "h", "i", "k", "l", "t", "w", "z"}
	endTime := time.Now()
	axis := BuildDiscreteAxis(startKey, endKeyList, uint64List, endTime)

	// first time.
	threshold1 := uint64(3)
	expectUint64List1 := []uint64{4, 0, 10, 2, 3, 0, 7, 11, 2}
	expectEndKeyList1 := []string{"a", "b", "d", "e", "i", "k", "l", "t", "z"}
	expectAxis1 := BuildDiscreteAxis(startKey, expectEndKeyList1, expectUint64List1, endTime)
	axis.DeNoise(threshold1)
	if !reflect.DeepEqual(axis, expectAxis1) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(expectAxis1), SprintDiscreteAxis(axis))
	}

	/**********************************************************/
	newAxis := BuildDiscreteAxis(startKey, endKeyList, uint64List, endTime)
	// second test.
	threshold2 := uint64(12)
	expectUint64List2 := []uint64{11}
	expectEndKeyList2 := []string{"z"}
	expectAxis2 := BuildDiscreteAxis(startKey, expectEndKeyList2, expectUint64List2, endTime)
	newAxis.DeNoise(threshold2)
	if !reflect.DeepEqual(newAxis, expectAxis2) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(expectAxis2), SprintDiscreteAxis(newAxis))
	}
}

func TestReSample(t *testing.T) {
	startKey := ""
	uint64List := []uint64{0, 0, 10, 2, 4, 3, 0, 7, 11, 2}
	endKeyList := []string{"a", "c", "d", "h", "i", "m", "q", "t", "x", "z"}
	endTime := time.Now()
	axis := BuildDiscreteAxis("\n", endKeyList, uint64List, endTime)

	desKeyList := []string{"a", "b", "c", "d", "f", "h", "i", "l", "m", "o", "p", "q", "t", "x", "z", "zz"}
	desUint64List := []uint64{2, 1, 0, 10, 3, 1, 5, 9, 3, 0, 2, 0, 3, 7, 2, 0}
	expectUint64List := []uint64{2, 1, 0, 10, 3, 2, 5, 9, 3, 0, 2, 0, 7, 11, 2, 0}
	desAxis := BuildDiscreteAxis(startKey, desKeyList, desUint64List, endTime)
	expectAxis := BuildDiscreteAxis(startKey, desKeyList, expectUint64List, endTime)

	axis.ReSample(desAxis)
	if !reflect.DeepEqual(desAxis, expectAxis) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(expectAxis), SprintDiscreteAxis(desAxis))
	}

	//对空轴的情况测试
	axis = BuildDiscreteAxis("\n2", []string{}, []uint64{}, endTime)
	expectAxis = desAxis.Clone()
	axis.ReSample(desAxis)
	if !reflect.DeepEqual(desAxis, expectAxis) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(expectAxis), SprintDiscreteAxis(desAxis))
	}
}

func TestDeProjection(t *testing.T) {
	startKey := "\n"
	uint64List := []uint64{0, 0, 10, 2, 4, 3, 0, 7, 11, 2}
	endKeyList := []string{"b", "c", "d", "h", "i", "m", "q", "t", "x", "z"}
	endTime := time.Now()
	axis := BuildDiscreteAxis(startKey, endKeyList, uint64List, endTime)

	desStartKey := ""
	desKeyList := []string{"a", "c", "d", "d", "f", "g", "m", "z"}
	desUint64List := []uint64{0, 0, 0, 0, 0, 0, 0, 0}

	expectUint64List := []uint64{0, 0, 10, 10, 2, 2, 4, 11}
	desAxis := BuildDiscreteAxis(desStartKey, desKeyList, desUint64List, endTime)
	expectAxis := BuildDiscreteAxis(desStartKey, desKeyList, expectUint64List, endTime)

	axis.DeProjection(desAxis)
	if !reflect.DeepEqual(desAxis, expectAxis) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(expectAxis), SprintDiscreteAxis(desAxis))
	}
}

func TestGetDiscreteKeys(t *testing.T) {
	startKey := ""
	uint64List := []uint64{0, 0, 10, 2, 4, 3, 0, 7, 11, 2}
	endKeyList := []string{"a", "c", "d", "h", "i", "m", "q", "t", "x", "z"}
	endTime := time.Now()
	axis := BuildDiscreteAxis(startKey, endKeyList, uint64List, endTime)

	expectKeys := DiscreteKeys{"", "a", "c", "d", "h", "i", "m", "q", "t", "x", "z"}
	keys := axis.GetDiscreteKeys()
	if !reflect.DeepEqual(keys, expectKeys) {
		t.Fatalf("expect %v, but got %v", expectKeys, keys)
	}

	// lines is empty
	axis.Lines = []*Line{}
	expectKeys = DiscreteKeys{""}
	keys = axis.GetDiscreteKeys()
	if !reflect.DeepEqual(keys, expectKeys) {
		t.Fatalf("expect %v, but got %v", expectKeys, keys)
	}
}

func TestRange(t *testing.T) {
	startKey := ""
	uint64List := []uint64{0, 0, 10, 2, 4, 3, 0, 7, 11, 2}
	endKeyList := []string{"a", "c", "d", "h", "i", "m", "q", "t", "x", "y"}
	endTime := time.Now()
	axis := BuildDiscreteAxis(startKey, endKeyList, uint64List, endTime)

	start := ""
	end := "z"
	expectAxis := axis.Clone()
	rangeAxis := axis.Range(start, end)
	if !reflect.DeepEqual(expectAxis, rangeAxis) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(expectAxis), SprintDiscreteAxis(rangeAxis))
	}

	start = ""
	end = "\n"
	expectAxis = &DiscreteAxis{
		StartKey: "",
		Lines:    []*Line{{axis.Lines[0].EndKey, axis.Lines[0].Clone()}},
		EndTime:  axis.EndTime,
	}
	rangeAxis = axis.Range(start, end)
	if !reflect.DeepEqual(expectAxis, rangeAxis) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(expectAxis), SprintDiscreteAxis(rangeAxis))
	}

	start = "b"
	end = "o"
	startIndex := 1
	endIndex := 7
	expectAxis = &DiscreteAxis{
		StartKey: "a",
		Lines:    make([]*Line, 0, endIndex-startIndex),
		EndTime:  axis.EndTime,
	}
	for i := startIndex; i < endIndex; i++ {
		expectAxis.Lines = append(expectAxis.Lines, &Line{axis.Lines[i].EndKey, axis.Lines[i].Clone()})
	}
	rangeAxis = axis.Range(start, end)
	if !reflect.DeepEqual(expectAxis, rangeAxis) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(expectAxis), SprintDiscreteAxis(rangeAxis))
	}

	start = "\n"
	end = "o"
	startIndex = 0
	endIndex = 7
	expectAxis = &DiscreteAxis{
		StartKey: "",
		Lines:    make([]*Line, 0, endIndex-startIndex),
		EndTime:  axis.EndTime,
	}
	for i := startIndex; i < endIndex; i++ {
		expectAxis.Lines = append(expectAxis.Lines, &Line{axis.Lines[i].EndKey, axis.Lines[i].Clone()})
	}
	rangeAxis = axis.Range(start, end)
	if !reflect.DeepEqual(expectAxis, rangeAxis) {
		t.Fatalf("expect\n%v\nbut got\n%v", SprintDiscreteAxis(expectAxis), SprintDiscreteAxis(rangeAxis))
	}
}
