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
	"testing"
)

func TestCreateValidAxis(t *testing.T) {
	keys := []string{"a", "b", "c", "d"}
	values := [][]uint64{{0, 1, 2}, {1, 2, 3}, {2, 3, 4}}
	axis := CreateAxis(keys, values)
	AssertEqual(t, axis.Keys, keys)
	AssertEqual(t, axis.ValuesList, values)
}

func TestCreateWithEmptyKeys(t *testing.T) {
	defer PanicExists(t)
	var keys []string
	values := [][]uint64{{0, 1, 2}, {1, 2, 3}, {2, 3, 4}}
	CreateAxis(keys, values)
}

func TestCreateWithOnlyOneKey(t *testing.T) {
	defer PanicExists(t)
	keys := []string{"a"}
	values := [][]uint64{{0, 1, 2}, {1, 2, 3}, {2, 3, 4}}
	CreateAxis(keys, values)
}

func TestCreateWithEmptyValues(t *testing.T) {
	defer PanicExists(t)
	keys := []string{"a", "b", "c", "d"}
	var values [][]uint64
	CreateAxis(keys, values)
}

func TestValuesLengthIsNotEqualToKeyLengthMinusOne(t *testing.T) {
	defer PanicExists(t)
	keys := []string{"a", "b", "c"}
	values := [][]uint64{{0, 1, 2}, {1, 2, 3}, {2, 3, 4}}
	CreateAxis(keys, values)
}

func TestCreateValidEmptyAxis(t *testing.T) {
	startKey := "a"
	endKey := "b"
	axis := CreateEmptyAxis(startKey, endKey, 10)
	AssertEqual(t, len(axis.Keys), 2)
	AssertEqual(t, len(axis.ValuesList), 10)
	for i := 0; i < len(axis.ValuesList); i++ {
		AssertEqual(t, len(axis.ValuesList[i]), 1)
	}
	AssertTwoDimUint64ArrayAllEqual(t, axis.ValuesList, 0)
}

func TestCreateEmptyAxisKeyReuse(t *testing.T) {
	axis1 := CreateEmptyAxis("a", "b", 10)
	axis2 := CreateEmptyAxis("a", "b", 10)
	AssertTrue(t, equal(axis1.Keys[0], axis2.Keys[0]))
	AssertTrue(t, equal(axis1.Keys[1], axis2.Keys[1]))
}

func TestRangeWithValidLeftAndRightBoarderGiven(t *testing.T) {
	keys := []string{"a", "b", "c", "d"}
	valueList := [][]uint64{{0, 1, 2}, {1, 2, 3}}
	axis := CreateAxis(keys, valueList)

	axisSearchResult := axis.Range("b", "c")
	AssertEqual(t, len(axisSearchResult.Keys), 2)
	AssertEqual(t, len(axisSearchResult.ValuesList), 2)
	for _, subList := range axisSearchResult.ValuesList {
		AssertEqual(t, len(subList), 1)
	}
	AssertArrayEqual(t,
		[]uint64{axisSearchResult.ValuesList[0][0], axisSearchResult.ValuesList[1][0]}, []uint64{1, 2})
}

func TestRangeWithValidLeftBoardGiven(t *testing.T) {
	keys := []string{"a", "b", "c", "d"}
	valueList := [][]uint64{{0, 1, 2}, {1, 2, 3}}
	axis := CreateAxis(keys, valueList)

	axisSearchResult := axis.Range("c", "")
	AssertEqual(t, len(axisSearchResult.Keys), 2)
	AssertEqual(t, len(axisSearchResult.ValuesList), 2)
	for _, subList := range axisSearchResult.ValuesList {
		AssertEqual(t, len(subList), 1)
	}
	AssertArrayEqual(t,
		[]uint64{axisSearchResult.ValuesList[0][0], axisSearchResult.ValuesList[1][0]},
		[]uint64{2, 3})
}

func TestRangeWithValidRightBorderGiven(t *testing.T) {
	keys := []string{"a", "b", "c", "d"}
	valueList := [][]uint64{{0, 1, 2}, {1, 2, 3}}
	axis := CreateAxis(keys, valueList)

	axisSearchResult := axis.Range("", "c")
	AssertEqual(t, len(axisSearchResult.Keys), 3)
	AssertEqual(t, len(axisSearchResult.ValuesList), 2)
	for _, subList := range axisSearchResult.ValuesList {
		AssertEqual(t, len(subList), 2)
	}
	AssertArrayEqual(t,
		[]uint64{axisSearchResult.ValuesList[0][0], axisSearchResult.ValuesList[1][0]},
		[]uint64{0, 1})
	AssertArrayEqual(t,
		[]uint64{axisSearchResult.ValuesList[0][1], axisSearchResult.ValuesList[1][1]},
		[]uint64{1, 2})
}

func TestInvalidLeftRightBorder(t *testing.T) {
	defer PanicExists(t)
	keys := []string{"a", "b", "c", "d"}
	valueList := [][]uint64{{0, 1, 2}, {1, 2, 3}}
	axis := CreateAxis(keys, valueList)
	axis.Range("a", "a")
}

func TestLeftAndRightBorderOutsideDataRange(t *testing.T) {
	keys := []string{"b", "c", "d", "e"}
	valueList := [][]uint64{{0, 1, 2}, {1, 2, 3}}
	axis := CreateAxis(keys, valueList)
	resultAxis := axis.Range("a", "f")
	AssertEqual(t, len(resultAxis.ValuesList), 2)
	for idx := range resultAxis.ValuesList {
		AssertEqual(t, len(resultAxis.ValuesList[idx]), 3)
	}
	AssertEqual(t, resultAxis.ValuesList[0], []uint64{0, 1, 2})
	AssertEqual(t, resultAxis.ValuesList[1], []uint64{1, 2, 3})
}

func TestSearchResultIsEmptyAxis(t *testing.T) {
	keys := []string{"b", "c", "d", "e"}
	valueList := [][]uint64{{0, 1, 2}, {1, 2, 3}}
	axis := CreateAxis(keys, valueList)
	resultAxis1 := axis.Range("a", "b")
	for idx := range resultAxis1.ValuesList {
		AssertArrayEqual(t, resultAxis1.ValuesList[idx], []uint64{0})
	}
	resultAxis2 := axis.Range("f", "")
	for idx := range resultAxis2.ValuesList {
		AssertEqual(t, resultAxis2.ValuesList[idx], []uint64{0})
	}
}
