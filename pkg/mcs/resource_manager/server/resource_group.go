// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package server provides a set of struct definitions for the resource group, can be imported.
package server

import (
	"encoding/json"
	"path"
	"sync"

	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"github.com/tikv/pd/server/storage"
	"go.uber.org/zap"
)

const (
	// SettingPrefix is the prefix of the setting key.
	SettingPrefix = "setting"
	// StatePrefix is the prefix of the state key.
	StatePrefix = "state"
)

// ResourceGroup is the definition of a resource group, for REST API.
type ResourceGroup struct {
	sync.RWMutex
	*rmpb.ResourceGroup
	ConsumptionRU       *rmpb.RequestUnitItem `json:"consumption_ru,omitempty"`
	ConsumptionResource *rmpb.ResourceItem    `json:"consumption_resource,omitempty"`
}

func (rg *ResourceGroup) String() string {
	res, err := json.Marshal(rg)
	if err != nil {
		log.Error("marshal resource group failed", zap.Error(err))
		return ""
	}
	return string(res)
}

// Copy copies the resource group.
func (rg *ResourceGroup) Copy() *ResourceGroup {
	// TODO: use a better way to copy
	rg.RLock()
	defer rg.RUnlock()
	res, err := json.Marshal(rg)
	if err != nil {
		panic(err)
	}
	var newRG ResourceGroup
	err = json.Unmarshal(res, &newRG)
	if err != nil {
		panic(err)
	}
	return &newRG
}

// CheckAndInit checks the validity of the resource group and initializes the default values if not setting.
// Only used to initialize the resource group when creating.
func (rg *ResourceGroup) CheckAndInit() error {
	if len(rg.Name) == 0 || len(rg.Name) > 32 {
		return errors.New("invalid resource group name, the length should be in [1,32]")
	}
	if rg.Mode != rmpb.GroupMode_RUMode && rg.Mode != rmpb.GroupMode_NativeMode {
		return errors.New("invalid resource group mode")
	}
	if rg.Mode == rmpb.GroupMode_RUMode {
		if rg.GetResourceSettings() != nil {
			return errors.New("invalid resource group settings, RU mode should not set resource settings")
		}
		if rg.GetRUSettings() == nil {
			rg.RUSettings = &rmpb.GroupRequestUnitSettings{}
		}
	}
	if rg.Mode == rmpb.GroupMode_NativeMode {
		if rg.GetRUSettings() != nil {
			return errors.New("invalid resource group settings, native mode should not set RU settings")
		}
		if rg.GetResourceSettings() == nil {
			rg.ResourceSettings = &rmpb.GroupResourceSettings{}
		}
	}
	return nil
}

// Reconfigure patches the resource group settings.
// Only used to patch the resource group when updating.
// Note: the tokens is the delta value to patch.
func (rg *ResourceGroup) Reconfigure(nrg *rmpb.ResourceGroup) error {
	rg.Lock()
	defer rg.Unlock()
	if nrg.GetMode() != rg.Mode {
		return errors.New("only support reconfigure in same mode, maybe you should delete and create a new one")
	}
	setTokens := func(state *rmpb.TokenBucketState, settings *rmpb.TokenBucketSettings) {
		if settings.DeltaTokens != 0 {
			state.CurrentTokens += settings.DeltaTokens
			settings.DeltaTokens = 0
		}
	}
	switch rg.Mode {
	case rmpb.GroupMode_RUMode:
		if nrg.GetResourceSettings() != nil {
			return errors.New("invalid resource group settings, RU mode should set RU settings")
		}
		if nrg.GetRUSettings() != nil {
			rg.RUSettings = nrg.GetRUSettings()
		}
		setTokens(rg.GetRUState().GetRRU(), nrg.GetRUSettings().GetRRU())
		setTokens(rg.GetRUState().GetWRU(), nrg.GetRUSettings().GetWRU())
	case rmpb.GroupMode_NativeMode:
		if nrg.GetRUSettings() != nil {
			return errors.New("invalid resource group settings, native mode should set resource settings")
		}
		if nrg.GetResourceSettings() != nil {
			rg.ResourceSettings = nrg.GetResourceSettings()
		}
		rg.GetResourceSettings()
		setTokens(rg.GetResourceState().GetCpu(), nrg.GetResourceSettings().GetCpu())
		setTokens(rg.GetResourceState().GetIoRead(), nrg.GetResourceSettings().GetIoRead())
		setTokens(rg.GetResourceState().GetIoWrite(), nrg.GetResourceSettings().GetIoWrite())
	}
	log.Info("patch resource group settings", zap.String("name", rg.Name), zap.String("settings", rg.String()))
	return nil
}

// TODO: add a txn to persist the resource group
func (rg *ResourceGroup) persist(engine storage.Storage) error {
	rg.RLock()
	defer rg.RUnlock()
	// state is high frequency
	nrg := rg.Copy()
	nrg.RUSettings = nil
	engine.SaveResourceGroup(path.Join(StatePrefix, nrg.Name), nrg)
	// settings is low frequency
	nrg = rg.Copy()
	nrg.RUState = nil
	engine.SaveResourceGroup(path.Join(SettingPrefix, nrg.Name), nrg)
	return nil
}
