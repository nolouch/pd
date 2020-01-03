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

import "sort"

func MemsetUint64(slice []uint64, v uint64) {
	sliceLen := len(slice)
	if sliceLen == 0 {
		return
	}
	slice[0] = v
	for bp := 1; bp < sliceLen; bp <<= 1 {
		copy(slice[bp:], slice[:bp])
	}
}

func MemsetInt(slice []int, v int) {
	sliceLen := len(slice)
	if sliceLen == 0 {
		return
	}
	slice[0] = v
	for bp := 1; bp < sliceLen; bp <<= 1 {
		copy(slice[bp:], slice[:bp])
	}
}

func GetLastKey(keys []string) string {
	return keys[len(keys)-1]
}

// Check `part` keys part of `src` keys
// Ps: Just make simple judgments
func CheckPartOf(src, part []string) {
	err := src[0] > part[0] || len(src) < len(part)
	srcLastKey := GetLastKey(src)
	partLastKey := GetLastKey(part)
	if srcLastKey != "" && (partLastKey == "" || srcLastKey < partLastKey) {
		err = true
	}
	if err {
		panic("The inclusion relationship is not satisfied between keys")
	}
}

func MakeKeys(keySet map[string]struct{}, unlimitedEnd bool) []string {
	keysLen := len(keySet)
	keys := make([]string, keysLen, keysLen+1)
	i := 0
	for key := range keySet {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	if unlimitedEnd {
		keys = append(keys, "")
	}
	return keys
}

// Max returns the larger of a and b.
func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func UInt64Max(a, b uint64) uint64 {
	if a > b {
		return a
	} else {
		return b
	}
}

// Min returns the smaller of a and b.
func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func UInt64Min(a, b uint64) uint64 {
	if a < b {
		return a
	} else {
		return b
	}
}
