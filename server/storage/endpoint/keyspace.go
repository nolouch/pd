// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package endpoint

import (
	"context"
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/storage/kv"
	"go.etcd.io/etcd/clientv3"
)

const (
	// spaceIDBase is base used to encode/decode spaceID.
	// It's set to 10 for better readability.
	spaceIDBase = 10
	// spaceIDBitSizeMax is the max bitSize of spaceID.
	// It's currently set to 24 (3bytes).
	spaceIDBitSizeMax = 24
)

// KeyspaceStorage defines storage operations on keyspace related data.
type KeyspaceStorage interface {
	// SaveNewKeyspace attempts to save keyspace meta and its id in a single transaction.
	SaveNewKeyspace(keyspace *keyspacepb.KeyspaceMeta) error
	// SaveKeyspace saves the given keyspace to the storage.
	SaveKeyspace(*keyspacepb.KeyspaceMeta) error
	// LoadKeyspace loads keyspace specified by spaceID.
	LoadKeyspace(spaceID uint32, keyspace *keyspacepb.KeyspaceMeta) (bool, error)
	// LoadRangeKeyspace loads no more than limit keyspaces starting at startID.
	LoadRangeKeyspace(startID uint32, limit int) ([]*keyspacepb.KeyspaceMeta, error)
	// LoadKeyspaceIDByName loads keyspace ID for the given keyspace specified by name.
	// It first constructs path to spaceID with the given name, then attempt to retrieve
	// target spaceID. If the target keyspace does not exist, result boolean is set to false.
	LoadKeyspaceIDByName(name string) (bool, uint32, error)
}

var _ KeyspaceStorage = (*StorageEndpoint)(nil)

// SaveNewKeyspace attempts to save keyspace meta and its id in a single transaction.
func (se *StorageEndpoint) SaveNewKeyspace(keyspace *keyspacepb.KeyspaceMeta) error {
	idPath := KeyspaceIDPath(keyspace.GetName())
	idVal := strconv.FormatUint(uint64(keyspace.GetId()), spaceIDBase)
	metaPath := KeyspaceMetaPath(keyspace.GetId())
	metaVal, err := proto.Marshal(keyspace)
	if err != nil {
		return errs.ErrProtoMarshal.Wrap(err).GenWithStackByCause()
	}
	return se.RunInTxn(context.TODO(), func(txn kv.Txn) error {
		if err = txn.Save(idPath, idVal); err != nil {
			return err
		}
		return txn.Save(metaPath, string(metaVal))
	})
}

// SaveKeyspace saves the given keyspace to the storage.
func (se *StorageEndpoint) SaveKeyspace(keyspace *keyspacepb.KeyspaceMeta) error {
	key := KeyspaceMetaPath(keyspace.GetId())
	return se.saveProto(key, keyspace)
}

// LoadKeyspace loads keyspace specified by spaceID.
func (se *StorageEndpoint) LoadKeyspace(spaceID uint32, keyspace *keyspacepb.KeyspaceMeta) (bool, error) {
	key := KeyspaceMetaPath(spaceID)
	return se.loadProto(key, keyspace)
}

// LoadRangeKeyspace loads keyspaces starting at startID.
// limit specifies the limit of loaded keyspaces.
func (se *StorageEndpoint) LoadRangeKeyspace(startID uint32, limit int) ([]*keyspacepb.KeyspaceMeta, error) {
	startKey := KeyspaceMetaPath(startID)
	endKey := clientv3.GetPrefixRangeEnd(KeyspaceMetaPrefix())
	keys, values, err := se.LoadRange(startKey, endKey, limit)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return []*keyspacepb.KeyspaceMeta{}, nil
	}
	keyspaces := make([]*keyspacepb.KeyspaceMeta, 0, len(keys))
	for _, value := range values {
		keyspace := &keyspacepb.KeyspaceMeta{}
		if err = proto.Unmarshal([]byte(value), keyspace); err != nil {
			return nil, err
		}
		keyspaces = append(keyspaces, keyspace)
	}
	return keyspaces, nil
}

// LoadKeyspaceIDByName loads keyspace ID for the given keyspace name
func (se *StorageEndpoint) LoadKeyspaceIDByName(name string) (bool, uint32, error) {
	key := KeyspaceIDPath(name)
	idStr, err := se.Load(key)
	// Failed to load the keyspaceID if loading operation errored, or if keyspace does not exist.
	if err != nil || idStr == "" {
		return false, 0, err
	}
	id64, err := strconv.ParseUint(idStr, spaceIDBase, spaceIDBitSizeMax)
	if err != nil {
		return false, 0, err
	}
	return true, uint32(id64), nil
}
