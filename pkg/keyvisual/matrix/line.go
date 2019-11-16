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

type Value interface {
	Split(count int) Value
	Merge(other Value)
	Less(threshold uint64, typ string) bool
	GetValue(typ string) uint64
	Clone() Value
	Reset()
	Default() Value
	Equal(other Value) bool
}

type Line struct {
	// StartKey string // EndKey from the previous Line
	EndKey string
	Value
}
