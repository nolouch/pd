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
	"compress/gzip"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/keyvisual/decorator"
	"github.com/pingcap/pd/pkg/keyvisual/matrix"
	"github.com/pingcap/pd/server"
	"go.uber.org/zap"
)

const (
	maxDisplayY    = 1536
	preThreshold   = 128
	preRatioTarget = 512
	preTarget      = 1024
)

var (
	defaultLayersConfig = LayersConfig{
		{Len: 60, Ratio: 2},                         // step 1 mins, total 30, 1 hour
		{Len: 60 / 2 * 24, Ratio: 30 / 2},           // step 2 mins, total 720, 1 day
		{Len: 60 / 30 * 24 * 7, Ratio: 5 * 60 / 30}, // step 30 mins, total 336, 1 week
		{Len: 24 * 30 / 5, Ratio: 0},                // step 5 hours, total 144, 1mount
	}

	defaultRegisterAPIGroupInfo = server.APIGroup{
		IsCore:  false,
		Name:    "keyvisual",
		Version: "v1",
	}
)

// Service provide the service of key visual web.
type Service struct {
	*http.ServeMux
	ctx      context.Context
	svr      *server.Server
	stats    *Stat
	strategy matrix.Strategy
}

// RegisterService register the key visual service to pd.
func RegisterService(svr *server.Server) (http.Handler, server.APIGroup) {
	labelStrategy := matrix.LabelStrategy(decorator.TiDBLabelStrategy{})
	// strategy := matrix.AverageStrategy(labelStrategy)
	strategy := matrix.DistanceStrategy(labelStrategy, math.Phi, 15)
	k := &Service{
		ServeMux: http.NewServeMux(),
		ctx:      context.TODO(),
		svr:      svr,
		stats:    NewStat(defaultLayersConfig, strategy),
		strategy: strategy,
	}

	k.HandleFunc("/pd/apis/keyvisual/v1/heatmaps", k.Heatmap)
	k.Run()
	return k, defaultRegisterAPIGroupInfo
}
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Info("DebugTop", zap.String("RequestURI", r.RequestURI))
	s.ServeMux.ServeHTTP(w, r)
}

func (s *Service) Run() {
	// go s.updateStatFromFiles()
	go s.updateStat(s.ctx)
}

func (s *Service) Heatmap(w http.ResponseWriter, r *http.Request) {
	log.Info("Start Services", zap.String("RequestURI", r.RequestURI))
	defer log.Info("End Service")

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
		if tsSec, err := strconv.ParseInt(startTimeString, 10, 64); err == nil {
			startTime = time.Unix(tsSec, 0)
		} else {
			log.Error("parse ts failed", zap.Error(err))
		}
	}
	if endTimeString != "" {
		if tsSec, err := strconv.ParseInt(endTimeString, 10, 64); err == nil {
			endTime = time.Unix(tsSec, 0)
		} else {
			log.Error("parse ts failed", zap.Error(err))
		}
	}

	log.Info("Request matrix",
		zap.Time("start-time", startTime),
		zap.Time("end-time", endTime),
		zap.String("start-key", startKey),
		zap.String("end-key", endKey),
	)

	// Fixme: return 403
	if !(startTime.Before(endTime) && (endKey == "" || startKey < endKey)) {
		panic("wrong range")
	}

	mx := s.stats.RangeMatrix(startTime, endTime, startKey, endKey, getTag(typ))
	var encoder *json.Encoder
	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer func() {
			if err := gz.Close(); err != nil {
				log.Warn("gzip close error", zap.Error(err))
			}
		}()
		encoder = json.NewEncoder(gz)
	} else {
		encoder = json.NewEncoder(w)
	}
	if err := encoder.Encode(mx); err != nil {
		log.Warn("json encode or write error", zap.Error(err))
	}
}

func (s *Service) updateStat(ctx context.Context) {
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
			regions, endTime := scanRegions(cluster)
			s.stats.Append(regions, endTime)
		}
	}
}
