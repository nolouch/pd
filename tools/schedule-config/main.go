// Copyright 2020 TiKV Project Authors.
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

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/pd/v4/server/config"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/kv"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/transport"
)

var (
	clusterID = flag.Uint64("cluster-id", 0, "please make cluster ID match with TiKV")
	storeID   = flag.Uint64("store-id", 0, "the store id need to delete store limit")
	endpoints = flag.String("endpoints", "http://127.0.0.1:2379", "endpoints urls")
	filePath  = flag.String("file", "stores.dump", "dump file path and name")
	caPath    = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs")
	certPath  = flag.String("cert", "", "path of file that contains X509 certificate in PEM format")
	keyPath   = flag.String("key", "", "path of file that contains X509 key in PEM format")
)

const (
	etcdTimeout     = 1200 * time.Second
	pdRootPath      = "/pd"
	minKVRangeLimit = 100
	clusterPath     = "raft"
)

var (
	rootPath = ""
)

func checkErr(err error) {
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func main() {
	flag.Parse()
	rootPath = path.Join(pdRootPath, strconv.FormatUint(*clusterID, 10))
	f, err := os.Create(*filePath)
	checkErr(err)
	defer f.Close()

	urls := strings.Split(*endpoints, ",")
	if len(urls) == 0 {
		fmt.Println("invalid url")
		os.Exit(1)
	}

	tlsInfo := transport.TLSInfo{
		CertFile:      *certPath,
		KeyFile:       *keyPath,
		TrustedCAFile: *caPath,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	checkErr(err)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   urls,
		DialTimeout: etcdTimeout,
		TLS:         tlsConfig,
	})
	checkErr(err)
	if err := validStoreID(urls[0]); err != nil {
		checkErr(err)
	}
	err = ResetStoreLimit(client)
	checkErr(err)
	fmt.Println("successful!")
}

func validStoreID(url string) error {
	urlStore := fmt.Sprintf("%s/pd/api/v1/store/%d", url, *storeID)
	resp, err := http.Get(urlStore)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	storeInfo := make(map[string]interface{})
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New(fmt.Sprintf("http get url %s return code %d", url, resp.StatusCode))
	}
	json.Unmarshal(b, &storeInfo)
	//fmt.Printf("%+v", storeInfo)
	if store, ok := storeInfo["store"]; ok {
		if id, iok := store.(map[string]interface{})["id"]; iok {
			sid := id.(float64)
			if uint64(sid) == *storeID {
				return errors.New("the store already exist, cannot remove store id.")
			}
		}
	}
	return nil
}

func ResetStoreLimit(client *clientv3.Client) error {
	c := &config.Config{}
	opt := config.NewPersistOptions(c)
	rootPath := path.Join(pdRootPath, strconv.FormatUint(*clusterID, 10))
	kvBase := kv.NewEtcdKVBase(client, rootPath)
	storage := core.NewStorage(kvBase)
	if err := opt.Reload(storage); err != nil {
		return err
	}
	cfg := opt.GetScheduleConfig().Clone()
	delete(cfg.StoreLimit, *storeID)
	opt.SetScheduleConfig(cfg)
	return opt.Persist(storage)
}
