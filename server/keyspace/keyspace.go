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

package keyspace

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/syncutil"
	"github.com/tikv/pd/server/id"
	"github.com/tikv/pd/server/storage/endpoint"
	"github.com/tikv/pd/server/storage/kv"
)

const (
	// AllocStep set idAllocator's step when write persistent window boundary.
	// Use a lower value for denser idAllocation in the event of frequent pd leader change.
	AllocStep = uint64(100)
	// AllocLabel is used to label keyspace idAllocator's metrics.
	AllocLabel = "keyspace-idAlloc"
	// DefaultKeyspaceName is the name reserved for default keyspace.
	DefaultKeyspaceName = "DEFAULT"
	// DefaultKeyspaceID is the id of default keyspace.
	DefaultKeyspaceID = uint32(0)
)

// Manager manages keyspace related data.
// It validates requests and provides concurrency control.
type Manager struct {
	// idLock guards keyspace name to id lookup entries.
	idLock syncutil.Mutex
	// metaLock guards keyspace meta.
	metaLock *syncutil.LockGroup
	// idAllocator allocates keyspace id.
	idAllocator id.Allocator
	// store is the storage for keyspace related information.
	store endpoint.KeyspaceStorage
	// ctx is the context of the manager, to be used in transaction.
	ctx context.Context
}

// CreateKeyspaceRequest represents necessary arguments to create a keyspace.
type CreateKeyspaceRequest struct {
	// Name of the keyspace to be created.
	// Using an existing name will result in error.
	Name   string
	Config map[string]string
	Now    time.Time
}

// NewKeyspaceManager creates a Manager of keyspace related data.
func NewKeyspaceManager(store endpoint.KeyspaceStorage, idAllocator id.Allocator) (*Manager, error) {
	manager := &Manager{
		store:       store,
		idAllocator: idAllocator,
		metaLock:    syncutil.NewLockGroup(syncutil.WithHash(SpaceIDHash)),
		ctx:         context.TODO(),
	}
	// Initialize default keyspace.
	now := time.Now()
	defaultKeyspace := &keyspacepb.KeyspaceMeta{
		Id:             DefaultKeyspaceID,
		Name:           DefaultKeyspaceName,
		State:          keyspacepb.KeyspaceState_ENABLED,
		CreatedAt:      now.Unix(),
		StateChangedAt: now.Unix(),
	}
	err := manager.saveNewKeyspace(defaultKeyspace)
	if err != nil && err != ErrKeyspaceExists && !errors.ErrorEqual(errs.ErrEtcdTxnConflict, err) {
		return nil, err
	}
	return manager, nil
}

// CreateKeyspace create a keyspace meta with given config and save it to storage.
func (manager *Manager) CreateKeyspace(request *CreateKeyspaceRequest) (*keyspacepb.KeyspaceMeta, error) {
	// Validate purposed name's legality.
	if err := validateName(request.Name); err != nil {
		return nil, err
	}
	// Allocate new keyspaceID.
	newID, err := manager.allocID()
	if err != nil {
		return nil, err
	}
	// Create and save keyspace metadata.
	keyspace := &keyspacepb.KeyspaceMeta{
		Id:             newID,
		Name:           request.Name,
		State:          keyspacepb.KeyspaceState_ENABLED,
		CreatedAt:      request.Now.Unix(),
		StateChangedAt: request.Now.Unix(),
		Config:         request.Config,
	}
	err = manager.saveNewKeyspace(keyspace)
	if err != nil {
		return nil, err
	}
	return keyspace, nil
}

func (manager *Manager) saveNewKeyspace(keyspace *keyspacepb.KeyspaceMeta) error {
	manager.idLock.Lock()
	defer manager.idLock.Unlock()
	manager.metaLock.Lock(keyspace.Id)
	defer manager.metaLock.Unlock(keyspace.Id)

	return manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		// Save keyspace ID.
		// Check if keyspace with that name already exists.
		nameExists, _, err := manager.store.LoadKeyspaceID(txn, keyspace.Name)
		if err != nil {
			return err
		}
		if nameExists {
			return ErrKeyspaceExists
		}
		err = manager.store.SaveKeyspaceID(txn, keyspace.Id, keyspace.Name)
		if err != nil {
			return err
		}
		// Save keyspace meta.
		// Check if keyspace with that id already exists.
		loadedMeta, err := manager.store.LoadKeyspaceMeta(txn, keyspace.Id)
		if err != nil {
			return err
		}
		if loadedMeta != nil {
			return ErrKeyspaceExists
		}
		return manager.store.SaveKeyspaceMeta(txn, keyspace)
	})
}

// LoadKeyspace returns the keyspace specified by name.
// It returns error if loading or unmarshalling met error or if keyspace does not exist.
func (manager *Manager) LoadKeyspace(name string) (*keyspacepb.KeyspaceMeta, error) {
	var meta *keyspacepb.KeyspaceMeta
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		loaded, spaceID, err := manager.store.LoadKeyspaceID(txn, name)
		if err != nil {
			return err
		}
		if !loaded {
			return ErrKeyspaceNotFound
		}
		meta, err = manager.store.LoadKeyspaceMeta(txn, spaceID)
		if err != nil {
			return err
		}
		if meta == nil {
			return ErrKeyspaceNotFound
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return meta, nil
}

// Mutation represents a single operation to be applied on keyspace config.
type Mutation struct {
	Op    OpType
	Key   string
	Value string
}

// OpType defines the type of keyspace config operation.
type OpType int

const (
	// OpPut denotes a put operation onto the given config.
	// If target key exists, it will put a new value,
	// otherwise, it creates a new config entry.
	OpPut OpType = iota + 1 // Operation type starts at 1.
	// OpDel denotes a deletion operation onto the given config.
	// Note: OpDel is idempotent, deleting a non-existing key
	// will not result in error.
	OpDel
)

// UpdateKeyspaceConfig changes target keyspace's config in the order specified in mutations.
// It returns error if saving failed, operation not allowed, or if keyspace not exists.
func (manager *Manager) UpdateKeyspaceConfig(name string, mutations []*Mutation) (*keyspacepb.KeyspaceMeta, error) {
	var meta *keyspacepb.KeyspaceMeta
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		// First get KeyspaceID from Name.
		loaded, spaceID, err := manager.store.LoadKeyspaceID(txn, name)
		if err != nil {
			return err
		}
		if !loaded {
			return ErrKeyspaceNotFound
		}
		manager.metaLock.Lock(spaceID)
		defer manager.metaLock.Unlock(spaceID)
		// Load keyspace by id.
		meta, err = manager.store.LoadKeyspaceMeta(txn, spaceID)
		if err != nil {
			return err
		}
		if meta == nil {
			return ErrKeyspaceNotFound
		}
		// Changing ARCHIVED keyspace's config is not allowed.
		if meta.State == keyspacepb.KeyspaceState_ARCHIVED {
			return errKeyspaceArchived
		}
		// Initialize meta's config map if it's nil.
		if meta.Config == nil {
			meta.Config = map[string]string{}
		}
		// Update keyspace config according to mutations.
		for _, mutation := range mutations {
			switch mutation.Op {
			case OpPut:
				meta.Config[mutation.Key] = mutation.Value
			case OpDel:
				delete(meta.Config, mutation.Key)
			default:
				return errIllegalOperation
			}
		}
		// Save the updated keyspace meta.
		return manager.store.SaveKeyspaceMeta(txn, meta)
	})

	if err != nil {
		return nil, err
	}
	return meta, nil
}

// UpdateKeyspaceState updates target keyspace to the given state if it's not already in that state.
// It returns error if saving failed, operation not allowed, or if keyspace not exists.
func (manager *Manager) UpdateKeyspaceState(name string, newState keyspacepb.KeyspaceState, now time.Time) (*keyspacepb.KeyspaceMeta, error) {
	// Changing the state of default keyspace is not allowed.
	if name == DefaultKeyspaceName {
		return nil, errModifyDefault
	}
	var meta *keyspacepb.KeyspaceMeta
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		// First get KeyspaceID from Name.
		loaded, spaceID, err := manager.store.LoadKeyspaceID(txn, name)
		if err != nil {
			return err
		}
		if !loaded {
			return ErrKeyspaceNotFound
		}
		manager.metaLock.Lock(spaceID)
		defer manager.metaLock.Unlock(spaceID)
		// Load keyspace by id.
		meta, err = manager.store.LoadKeyspaceMeta(txn, spaceID)
		if err != nil {
			return err
		}
		if meta == nil {
			return ErrKeyspaceNotFound
		}
		// If keyspace is already in target state, then nothing needs to be change.
		if meta.State == newState {
			return nil
		}
		// ARCHIVED is the terminal state that cannot be changed from.
		if meta.State == keyspacepb.KeyspaceState_ARCHIVED {
			return errKeyspaceArchived
		}
		// Archiving an enabled keyspace directly is not allowed.
		if meta.State == keyspacepb.KeyspaceState_ENABLED && newState == keyspacepb.KeyspaceState_ARCHIVED {
			return errArchiveEnabled
		}
		// Change keyspace state and record change time.
		meta.StateChangedAt = now.Unix()
		meta.State = newState
		return manager.store.SaveKeyspaceMeta(txn, meta)
	})
	if err != nil {
		return nil, err
	}
	return meta, nil
}

// LoadRangeKeyspace load up to limit keyspaces starting from keyspace with startID.
func (manager *Manager) LoadRangeKeyspace(startID uint32, limit int) ([]*keyspacepb.KeyspaceMeta, error) {
	// Load Start should fall within acceptable ID range.
	if startID > spaceIDMax {
		return nil, errors.Errorf("startID of the scan %d exceeds spaceID Max %d", startID, spaceIDMax)
	}
	return manager.store.LoadRangeKeyspace(startID, limit)
}

// allocID allocate a new keyspace id.
func (manager *Manager) allocID() (uint32, error) {
	id64, err := manager.idAllocator.Alloc()
	if err != nil {
		return 0, err
	}
	id32 := uint32(id64)
	if err = validateID(id32); err != nil {
		return 0, err
	}
	return id32, nil
}
