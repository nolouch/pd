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

import "encoding/hex"

type splitTag int

const (
	splitTo  splitTag = iota // Direct assignment after split
	splitAdd                 // Add to original value after split
)

type splitStrategy interface {
	GenerateHelper(chunks []chunk, compactKeys []string) interface{}
	Split(dst, src chunk, axesIndex int, helper interface{}, tag splitTag)
}

// implemented in the decorator package
type LabelStrategy interface {
	CrossBorder(startKey, endKey string) bool
	Label(key string) LabelKey
}

// The complete interface required to call Pixel
type Strategy interface {
	splitStrategy
	LabelStrategy
}

// One of the simplest LabelStrategy
// Can be used when unit testing
type NaiveLabelStrategy struct{}

func (s NaiveLabelStrategy) CrossBorder(_, _ string) bool {
	return false
}

func (s NaiveLabelStrategy) Label(key string) LabelKey {
	str := hex.EncodeToString([]byte(key))
	return LabelKey{
		Key:    str,
		Labels: []string{str},
	}
}
