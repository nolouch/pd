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
