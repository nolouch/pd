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
	"reflect"
	"testing"
	"time"
)

// Implementation of a Value interface for testing

type valueUint64 struct {
	uint64
}

var zeroValueUint64 Value = &valueUint64{0}

func (v *valueUint64) Clone() Value {
	return &valueUint64{v.uint64}
}

func (v *valueUint64) GetValue(_ ValueTag) uint64 {
	return v.uint64
}

func (v *valueUint64) Equal(other Value) bool {
	v2 := other.(*valueUint64)
	return v.uint64 == v2.uint64
}

func (v *valueUint64) Split(count int) Value {
	return &valueUint64{v.uint64 / uint64(count)}
}

func (v *valueUint64) Merge(other Value) {
	v2 := other.(*valueUint64)
	v.uint64 += v2.uint64
}

func (axis *DiscreteAxis) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("StartKey: %v\n", axis.StartKey))
	for _, line := range axis.Lines {
		buf.WriteString(fmt.Sprintf("[%v, %v]", line.GetValue(INTEGRATION), line.EndKey))
	}
	buf.WriteString(fmt.Sprintf("\nEndTime: %v\n", axis.EndTime))
	return buf.String()
}

func BuildDiscreteAxis(startKey string, keys []string, values []uint64, endTime time.Time) *DiscreteAxis {
	lines := make([]*Line, len(values))
	for i, value := range values {
		lines[i] = &Line{
			EndKey: keys[i],
			Value:  &valueUint64{value},
		}
	}
	return &DiscreteAxis{
		StartKey: startKey,
		Lines:    lines,
		EndTime:  endTime,
	}
}

func AssertEq(t *testing.T, dstAxis *DiscreteAxis, expectAxis *DiscreteAxis) {
	if !reflect.DeepEqual(dstAxis, expectAxis) {
		t.Fatalf("expect\n%v\nbut got\n%v", expectAxis.String(), dstAxis.String())
	}
}

func AssertNe(t *testing.T, dstAxis *DiscreteAxis, expectAxis *DiscreteAxis) {
	if reflect.DeepEqual(dstAxis, expectAxis) {
		t.Fatalf("expect not \n%v\nbut got it", dstAxis.String())
	}
}

func TestNewZeroAxis(t *testing.T) {
	startKey := ""
	endKeyList := []string{"a", "b", "c", "d", "e"}
	valueList := []uint64{0, 0, 0, 0, 0}
	endTime := time.Now()

	keySet := make(map[string]struct{})
	keySet[startKey] = struct{}{}
	for _, key := range endKeyList {
		keySet[key] = struct{}{}
	}
	// endUnlimited = false
	expectAxis := BuildDiscreteAxis(startKey, endKeyList, valueList, endTime)
	dstAxis := NewZeroAxis(endTime, keySet, false, zeroValueUint64)
	AssertEq(t, dstAxis, expectAxis)
	// endUnlimited = true
	endKeyList = append(endKeyList, "")
	valueList = append(valueList, 0)
	expectAxis = BuildDiscreteAxis(startKey, endKeyList, valueList, endTime)
	dstAxis = NewZeroAxis(endTime, keySet, true, zeroValueUint64)
	AssertEq(t, dstAxis, expectAxis)
}

func TestDiscreteAxis_Clone(t *testing.T) {
	startKey := ""
	valueList := []uint64{0, 0, 10, 2, 4, 3, 0, 7, 11, 2}
	zeroValueList := []uint64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	endKeyList := []string{"a", "b", "d", "e", "h", "i", "k", "l", "t", "z"}
	endTime := time.Now()
	// zeroValue = nil
	axis := BuildDiscreteAxis(startKey, endKeyList, valueList, endTime)
	axisClone := axis.Clone(nil)
	AssertEq(t, axisClone, axis)

	axisClone.Lines[0].Merge(&valueUint64{100})
	AssertNe(t, axisClone, axis)
	// zeroValue != nil
	zeroAxis := BuildDiscreteAxis(startKey, endKeyList, zeroValueList, endTime)
	axisClone = axis.Clone(zeroValueUint64)
	AssertEq(t, axisClone, zeroAxis)
}

func TestDiscreteAxis_GetDiscreteKeys(t *testing.T) {
	startKey := ""
	valueList := []uint64{0, 0, 10, 2, 4, 3, 0, 7, 11, 2}
	endKeyList := []string{"a", "c", "d", "h", "i", "m", "q", "t", "x", "z"}
	endTime := time.Now()
	axis := BuildDiscreteAxis(startKey, endKeyList, valueList, endTime)

	expectKeys := DiscreteKeys{"", "a", "c", "d", "h", "i", "m", "q", "t", "x", "z"}
	keys := axis.GetDiscreteKeys()
	if !reflect.DeepEqual(keys, expectKeys) {
		t.Fatalf("expect %v, but got %v", expectKeys, keys)
	}
}

func TestDiscreteAxis_SplitReSample(t *testing.T) {
	startKey := ""
	valueList := []uint64{0, 0, 10, 2, 4, 3, 0, 7, 11, 2}
	endKeyList := []string{"a", "c", "d", "h", "i", "m", "q", "t", "x", "z"}
	endTime := time.Now()
	axis := BuildDiscreteAxis(startKey, endKeyList, valueList, endTime)

	dstKeyList := []string{"a", "b", "c", "d", "f", "h", "i", "l", "m", "o", "p", "q", "t", "x", "z", "zz"}
	dstValueList := []uint64{2, 1, 0, 10, 3, 1, 5, 9, 3, 0, 2, 0, 3, 7, 2, 0}
	expectValueList := []uint64{2, 1, 0, 20, 4, 2, 9, 10, 4, 0, 2, 0, 10, 18, 4, 0}
	dstAxis := BuildDiscreteAxis(startKey, dstKeyList, dstValueList, endTime)
	expectAxis := BuildDiscreteAxis(startKey, dstKeyList, expectValueList, endTime)

	axis.SplitReSample(dstAxis)
	AssertEq(t, dstAxis, expectAxis)
}

func TestDiscreteAxis_MergeReSample(t *testing.T) {
	startKey := ""
	valueList := []uint64{0, 0, 10, 2, 4, 3, 0, 7, 11, 2}
	endKeyList := []string{"a", "c", "d", "h", "i", "m", "q", "t", "x", "z"}
	endTime := time.Now()
	axis := BuildDiscreteAxis(startKey, endKeyList, valueList, endTime)

	dstKeys := DiscreteKeys{"", "a", "h", "t", "z"}
	var dstKeyList []string = dstKeys[1:]
	expectValueList := []uint64{0, 12, 14, 13}
	axis.MergeReSample(dstKeys)
	expectAxis := BuildDiscreteAxis(startKey, dstKeyList, expectValueList, endTime)

	AssertEq(t, axis, expectAxis)
}

func TestDiscreteAxis_DeNoise(t *testing.T) {
	startKey := ""
	endKeyList := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14"}
	endTime := time.Now()

	assertRowsEq := func(dst, expect int) {
		if dst != expect {
			t.Fatalf("expect %d, but got %d", expect, dst)
		}
	}

	// Evenly distributed
	valueList := []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	axis := BuildDiscreteAxis(startKey, endKeyList, valueList, endTime)

	axisClone := axis.Clone(nil)
	AssertEq(t, axisClone, axis)
	assertRowsEq(axisClone.DeNoise(INTEGRATION, 3, 0, 0), 5)

	expectKeyList := []string{"2", "5", "8", "11", "14"}
	expectValueList := []uint64{3, 3, 3, 3, 3}
	expectAxis := BuildDiscreteAxis(startKey, expectKeyList, expectValueList, endTime)
	dstAxis := axis.Clone(nil)
	dstAxis.DeNoise(INTEGRATION, 3, 4, 5)
	AssertEq(t, dstAxis, expectAxis)

	expectKeyList = []string{"1", "3", "5", "7", "9", "11", "13", "14"}
	expectValueList = []uint64{2, 2, 2, 2, 2, 2, 2, 1}
	expectAxis = BuildDiscreteAxis(startKey, expectKeyList, expectValueList, endTime)
	dstAxis = axis.Clone(nil)
	dstAxis.DeNoise(INTEGRATION, 3, 2, 5)
	AssertEq(t, dstAxis, expectAxis)

	// hot spot
	valueList = []uint64{0, 0, 0, 99, 100, 101, 0, 0, 1, 1, 1, 100000, 1, 0, 10}
	axis = BuildDiscreteAxis(startKey, endKeyList, valueList, endTime)

	assertRowsEq(axis.DeNoise(INTEGRATION, 3, 0, 0), 8)
	assertRowsEq(axis.DeNoise(INTEGRATION, 20, 0, 0), 7)
	assertRowsEq(axis.DeNoise(INTEGRATION, 1000, 0, 0), 3)

	expectKeyList = []string{"1", "2", "3", "4", "5", "7", "9", "10", "11", "13", "14"}
	expectValueList = []uint64{0, 0, 99, 100, 101, 0, 2, 1, 100000, 1, 10}
	expectAxis = BuildDiscreteAxis(startKey, expectKeyList, expectValueList, endTime)
	dstAxis = axis.Clone(nil)
	dstAxis.DeNoise(INTEGRATION, 3, 2, 5)
	AssertEq(t, dstAxis, expectAxis)

	expectKeyList = []string{"2", "3", "4", "5", "9", "10", "11", "14"}
	expectValueList = []uint64{0, 99, 100, 101, 2, 1, 100000, 11}
	expectAxis = BuildDiscreteAxis(startKey, expectKeyList, expectValueList, endTime)
	dstAxis = axis.Clone(nil)
	dstAxis.DeNoise(INTEGRATION, 20, 4, 5)
	AssertEq(t, dstAxis, expectAxis)

	expectKeyList = []string{"2", "5", "8", "10", "11", "14"}
	expectValueList = []uint64{0, 300, 1, 2, 100000, 11}
	expectAxis = BuildDiscreteAxis(startKey, expectKeyList, expectValueList, endTime)
	dstAxis = axis.Clone(nil)
	dstAxis.DeNoise(INTEGRATION, 1000, 3, 5)
	AssertEq(t, dstAxis, expectAxis)
}

func TestDiscreteAxis_Divide(t *testing.T) {
	startKey := ""
	endKeyList := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14"}
	endTime := time.Now()

	assertKeysEq := func(dst, expect DiscreteKeys) {
		if !reflect.DeepEqual(dst, expect) {
			t.Fatalf("expect %v, but got %v", expect, dst)
		}
	}

	// Evenly distributed
	valueList := []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	axis := BuildDiscreteAxis(startKey, endKeyList, valueList, endTime)

	dstKeys := axis.Divide(8, INTEGRATION, zeroValueUint64)
	expectKeys := DiscreteKeys{"", "2", "5", "8", "11", "14"}
	assertKeysEq(dstKeys, expectKeys)

	dstKeys = axis.Divide(20, INTEGRATION, zeroValueUint64)
	expectKeys = DiscreteKeys{"", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14"}
	assertKeysEq(dstKeys, expectKeys)

	// hot spot
	valueList = []uint64{0, 0, 0, 99, 100, 101, 0, 0, 1, 1, 1, 100000, 1, 0, 10}
	axis = BuildDiscreteAxis(startKey, endKeyList, valueList, endTime)

	dstKeys = axis.Divide(8, INTEGRATION, zeroValueUint64)
	expectKeys = DiscreteKeys{"", "4", "5", "10", "11", "14"}
	assertKeysEq(dstKeys, expectKeys)

	dstKeys = axis.Divide(5, INTEGRATION, zeroValueUint64)
	expectKeys = DiscreteKeys{"", "7", "10", "11", "14"}
	assertKeysEq(dstKeys, expectKeys)

	dstKeys = axis.Divide(20, INTEGRATION, zeroValueUint64)
	expectKeys = DiscreteKeys{"", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14"}
	assertKeysEq(dstKeys, expectKeys)
}

func TestDiscreteAxis_Range(t *testing.T) {
	startKey := ""
	valueList := []uint64{0, 0, 10, 2, 4, 3, 0, 7, 11, 2}
	endKeyList := []string{"a", "c", "d", "h", "i", "m", "q", "t", "x", "y"}
	endTime := time.Now()
	axis := BuildDiscreteAxis(startKey, endKeyList, valueList, endTime)

	dstAxis := axis.Range("", "z", zeroValueUint64)
	expectAxis := axis.Clone(nil)
	AssertEq(t, dstAxis, expectAxis)

	dstAxis = axis.Range("", "0", zeroValueUint64)
	expectAxis = BuildDiscreteAxis("", []string{"a"}, []uint64{0}, endTime)
	AssertEq(t, dstAxis, expectAxis)

	dstAxis = axis.Range("t", "", zeroValueUint64)
	expectAxis = BuildDiscreteAxis("t", []string{"x", "y"}, []uint64{11, 2}, endTime)
	AssertEq(t, dstAxis, expectAxis)

	dstAxis = axis.Range("z", "", zeroValueUint64)
	expectAxis = BuildDiscreteAxis("z", []string{""}, []uint64{0}, endTime)
	AssertEq(t, dstAxis, expectAxis)

	dstAxis = axis.Range("t", "x", zeroValueUint64)
	expectAxis = BuildDiscreteAxis("t", []string{"x"}, []uint64{11}, endTime)
	AssertEq(t, dstAxis, expectAxis)

	dstAxis = axis.Range("b", "j", zeroValueUint64)
	expectAxis = BuildDiscreteAxis("a", []string{"c", "d", "h", "i", "m"}, []uint64{0, 10, 2, 4, 3}, endTime)
	AssertEq(t, dstAxis, expectAxis)

	dstAxis = axis.Range("0", "b", zeroValueUint64)
	expectAxis = BuildDiscreteAxis("", []string{"a", "c"}, []uint64{0, 0}, endTime)
	AssertEq(t, dstAxis, expectAxis)
}
