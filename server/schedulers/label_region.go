// Copyright 2018 PingCAP, Inc.
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

package schedulers

import (
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/checker"
	"github.com/pingcap/pd/server/schedule/filter"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/opt"
	"github.com/pingcap/pd/server/schedule/selector"
	"go.uber.org/zap"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("label-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})

	schedule.RegisterScheduler("label-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newLabelScheduler(opController), nil
	})
}

const labelRegionSchedulerName = "label-region-scheduler"

type labelRegionScheduler struct {
	name string
	*baseScheduler
	selector *selector.BalanceSelector
}

// LabelRegionScheduler is mainly based on the store's label information for scheduling.
// Now only used for reject leader schedule, that will move the leader out of
// the store with the specific label.
func newRegionLabelScheduler(opController *schedule.OperatorController) schedule.Scheduler {
	filters := []filter.Filter{
		filter.StoreStateFilter{ActionScope: labelRegionSchedulerName, TransferLeader: true},
	}
	kind := core.NewScheduleKind(core.LeaderKind, core.ByCount)
	return &labelRegionScheduler{
		name:          labelRegionSchedulerName,
		baseScheduler: newBaseScheduler(opController),
		selector:      selector.NewBalanceSelector(kind, filters),
	}
}

func (s *labelRegionScheduler) GetName() string {
	return s.name
}

func (s *labelRegionScheduler) GetType() string {
	return "label-region"
}

func (s *labelRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (s *labelRegionScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	stores := cluster.GetStores()
	rejectRegionStores := make(map[uint64]struct{})
	for _, s := range stores {
		if cluster.CheckLabelProperty(opt.RejectRegion, s.GetLabels()) {
			rejectRegionStores[s.GetID()] = struct{}{}
		}
	}
	if len(rejectRegionStores) == 0 {
		schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
		return nil
	}
	log.Debug("label scheduler reject leader store list", zap.Reflect("stores", rejectRegionStores))
	for id := range rejectRegionStores {
		if region := cluster.RandLeaderRegion(id); region != nil {
			log.Debug("label scheduler selects region to transfer leader", zap.Uint64("region-id", region.GetID()))
			excludeStores := make(map[uint64]struct{})
			for _, p := range region.GetDownPeers() {
				excludeStores[p.GetPeer().GetStoreId()] = struct{}{}
			}
			for _, p := range region.GetPendingPeers() {
				excludeStores[p.GetStoreId()] = struct{}{}
			}
			f := filter.NewExcludedFilter(s.GetName(), nil, excludeStores)
			target := s.selector.SelectTarget(cluster, cluster.GetFollowerStores(region), f)
			if target == nil {
				log.Debug("label scheduler no target found for region", zap.Uint64("region-id", region.GetID()))
				schedulerCounter.WithLabelValues(s.GetName(), "no-target").Inc()
				continue
			}

			schedulerCounter.WithLabelValues(s.GetName(), "new-operator").Inc()
			op := operator.CreateTransferLeaderOperator("label-reject-leader", region, id, target.GetID(), operator.OpLeader)
			return []*operator.Operator{op}
		}
	}

	for id := range rejectRegionStores {
		region := cluster.RandFollowerRegion(id)
		if region == nil {
			continue
		}
		oldPeer := region.GetStorePeer(id)
		checker := checker.NewReplicaChecker(cluster, nil, s.GetName())
		filter := filter.StoreStateFilter{ActionScope: labelRegionSchedulerName, MoveRegion: true}
		storeID, _ := checker.SelectBestReplacementStore(region, oldPeer, filter)
		newPeer, err := cluster.AllocPeer(storeID)
		if err != nil {
			schedulerCounter.WithLabelValues(s.GetName(), "no-peer").Inc()
			return nil
		}
		op, err := operator.CreateMovePeerOperator("balance-region", cluster, region, operator.OpBalance, oldPeer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
		if err != nil {
			schedulerCounter.WithLabelValues(s.GetName(), "create-operator-fail").Inc()
			return nil

		}
		schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
		return []*operator.Operator{op}
	}

	schedulerCounter.WithLabelValues(s.GetName(), "no-region").Inc()
	return nil
}
