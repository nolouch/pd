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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type dbInfo struct {
	Name struct {
		O string `json:"O"`
		L string `json:"L"`
	} `json:"db_name"`
	State int `json:"state"`
}

type tableInfo struct {
	ID   int64 `json:"id"`
	Name struct {
		O string `json:"O"`
		L string `json:"L"`
	} `json:"name"`
	Indices []struct {
		ID   int64 `json:"id"`
		Name struct {
			O string `json:"O"`
			L string `json:"L"`
		} `json:"idx_name"`
	} `json:"index_info"`
}

func request(addr string, uri string, v interface{}) {
	resp, err := http.Get(fmt.Sprintf("%s/%s", addr, uri))
	perr(err)
	r, err := ioutil.ReadAll(resp.Body)
	perr(err)
	err = resp.Body.Close()
	perr(err)
	err = json.Unmarshal([]byte(r), v)
	perr(err)
}

func dbRequest(limit uint64) []*dbInfo {
	var dbInfos = make([]*dbInfo, limit)
	//request(*tidbAddr, "schema", &dbInfos)
	return dbInfos
}

func tableRequest(limit uint64, s string) []*tableInfo {
	var tableInfos = make([]*tableInfo, limit)
	//uri := fmt.Sprintf("schema/%s", s)
	//request(*tidbAddr, uri, &tableInfos)
	return tableInfos
}
