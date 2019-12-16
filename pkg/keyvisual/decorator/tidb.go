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

package decorator

import (
	"encoding/hex"
	"fmt"
	"sort"
	"sync"

	"github.com/pingcap/pd/pkg/codec"
	"github.com/pingcap/pd/pkg/keyvisual/matrix"
)

// Table saves the info of a table
type Table struct {
	Name string
	DB   string
	ID   int64

	Indices map[int64]string
}

func (t *Table) String() string {
	return fmt.Sprintf("%s.%s", t.DB, t.Name)
}

// TableSlice is the slice of tables
type TableSlice []*Table

func (s TableSlice) Len() int      { return len(s) }
func (s TableSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s TableSlice) Less(i, j int) bool {
	if s[i].DB < s[j].DB {
		return true
	} else if s[i].DB == s[j].DB && s[i].Name < s[j].Name {
		return true
	} else if s[i].DB == s[j].DB && s[i].Name == s[j].Name {
		return s[i].ID < s[j].ID
	}
	return false
}

// id -> map
var tables = sync.Map{}

func loadTables() []*Table {
	tableSlice := make([]*Table, 0, 1024)

	tables.Range(func(_key, value interface{}) bool {
		table := value.(*Table)
		tableSlice = append(tableSlice, table)
		return true
	})

	sort.Sort(TableSlice(tableSlice))
	return tableSlice
}

func updateTables() {
	dbInfos := dbRequest(0)
	for _, info := range dbInfos {
		if info.State == 0 {
			continue
		}
		tblInfos := tableRequest(0, info.Name.O)

		for _, table := range tblInfos {
			indices := make(map[int64]string, len(table.Indices))
			for _, index := range table.Indices {
				indices[index.ID] = index.Name.O
			}
			newTable := &Table{
				ID:      table.ID,
				Name:    table.Name.O,
				DB:      info.Name.O,
				Indices: indices,
			}
			tables.Store(table.ID, newTable)
		}
	}
}

// TiDBLabelStrategy implements the LabelStrategy interface. Get Label Information from TiDB.
type TiDBLabelStrategy struct{}

func (_ TiDBLabelStrategy) CrossBorder(startKey, endKey string) bool {
	// TODO: Unsafe conversion
	startBytes, endBytes := codec.Key(Bytes(startKey)), codec.Key(Bytes(endKey))
	startIsMeta, startTableID := startBytes.MetaOrTable()
	endIsMeta, endTableID := endBytes.MetaOrTable()
	if startIsMeta || endIsMeta {
		return startIsMeta != endIsMeta
	}
	if startTableID != endTableID {
		return true
	}
	startIndex := startBytes.IndexID()
	endIndex := endBytes.IndexID()
	return startIndex != endIndex
}

// Fixme: the label information should get from tidb.
func (_ TiDBLabelStrategy) Label(key string) (label matrix.LabelKey) {
	// TODO: Unsafe conversion
	keyBytes := []byte(key)
	label.Key = hex.EncodeToString(keyBytes)
	decodeKey := codec.Key(keyBytes)
	isMeta, TableID := decodeKey.MetaOrTable()
	if isMeta {
		label.Labels = append(label.Labels, "meta")
	} else {
		label.Labels = append(label.Labels, fmt.Sprintf("table_%d", TableID))
		if rowID := decodeKey.RowID(); rowID != 0 {
			label.Labels = append(label.Labels, fmt.Sprintf("row_%d", rowID))
		} else if indexID := decodeKey.IndexID(); indexID != 0 {
			label.Labels = append(label.Labels, fmt.Sprintf("index_%d", indexID))
		}
	}
	return
}
