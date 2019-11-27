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

package keyvisual

import (
	"context"
	"encoding/json"
	"flag"
	"net/http"
	"strconv"
	"time"

	assetfs "github.com/elazarl/go-bindata-assetfs"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/keyvisual/decorator"
	"github.com/pingcap/pd/server"
	"go.uber.org/zap"
)

var (
	addr      = flag.String("addr", "0.0.0.0:8001", "Listening address")
	pdAddr    = flag.String("pd", "http://127.0.0.1:2379", "PD address")
	tidbAddr  = flag.String("tidb", "http://127.0.0.1:10080", "TiDB Address")
	ignoreSys = flag.Bool("no-sys", true, "Ignore system database")
)

// KeyvisualService provide the service of key visual web.
type KeyvisualService struct {
	*http.ServeMux
	ctx   context.Context
	svr   *server.Server
	stats *Stat
}

var (
	defaultLayersConfig = LayersConfig{
		{Len: 60 * 24, Ratio: 10},
		{Len: 60 / 10 * 24 * 15, Ratio: 6},
		{Len: 24 * 15, Ratio: 24},
		{Len: 0, Ratio: 0},
	}

	defaultRegisterAPIGroupInfo = server.APIGroupInfo{
		IsCore:  false,
		Group:   "keyvisual",
		Version: "v1",
	}
)

// RegisterKeyvisualService register the service to pd.
func RegisterKeyvisualService(svr *server.Server) (http.Handler, server.APIGroupInfo) {
	ctx := context.TODO()
	mux := http.NewServeMux()
	stats := NewStat(defaultLayersConfig)
	k := &KeyvisualService{
		ServeMux: mux,
		ctx:      ctx,
		svr:      svr,
		stats:    stats,
	}
	fileServer := http.FileServer(&assetfs.AssetFS{
		Asset:     Asset,
		AssetDir:  AssetDir,
		AssetInfo: AssetInfo,
		Prefix:    "/public",
	})
	k.Handle("/pd/apis/keyvisual/v1/ui/", http.StripPrefix("/pd/apis/keyvisual/v1/ui/", fileServer))
	k.HandleFunc("/pd/apis/keyvisual/v1/heatmaps", k.Heatmap)
	k.Run()
	return k, defaultRegisterAPIGroupInfo
}

func (s *KeyvisualService) Run() {
	go s.updateStat(s.ctx)
}

func (s *KeyvisualService) Heatmap(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-type", "application/json")
	form := r.URL.Query()
	startKey := form.Get("startkey")
	endKey := form.Get("endkey")
	startTimeString := form.Get("starttime")
	endTimeString := form.Get("endtime")
	typ := form.Get("type")
	endTime := time.Now()
	startTime := endTime.Add(-1200 * time.Minute)

	if startTimeString != "" {
		tsSec, err := strconv.ParseInt(startTimeString, 10, 64)
		if err != nil {
			log.Error("parse ts failed", zap.Error(err))
		}
		startTime = time.Unix(tsSec, 0)
	}
	if endTimeString != "" {
		tsSec, err := strconv.ParseInt(endTimeString, 10, 64)
		if err != nil {
			log.Error("parse ts failed", zap.Error(err))
		}
		endTime = time.Unix(tsSec, 0)
	}

	log.Info("Request matrix",
		zap.Time("start-time", startTime),
		zap.Time("end-time", endTime),
		zap.String("start-key", startKey),
		zap.String("end-key", endKey),
	)
	matrix := s.stats.RangeMatrix(startTime, endTime, startKey, endKey, GetTag(typ))
	data, _ := json.Marshal(decorator.RangeTableID(matrix))
	_, err := w.Write(data)
	perr(err)
}

func (s *KeyvisualService) updateStat(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cluster := s.svr.GetRaftCluster()
			if cluster == nil {
				continue
			}
			regions := scanRegions(cluster)
			s.stats.Append(regions)
		}
	}
}
