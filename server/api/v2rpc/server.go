// Copyright 2017 PingCAP, Inc.
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

// TODO: move conn.go to this package.

package v2rpc

import (
	"io"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	pb "github.com/pingcap/kvproto/pkg/pdpb2"
	"github.com/pingcap/pd/server"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// TODO: check each requests header.

// RPCServer provide gRPC services.
type RPCServer struct {
	*server.Server
}

// NewRPCServer create a new RPCServer.
func NewRPCServer(svr *server.Server) *RPCServer {
	return &RPCServer{
		Server: svr,
	}
}

func (s *RPCServer) RegisterTo(gs *grpc.Server) {
	pb.RegisterPDServer(gs, s)
}

func togRPCError(err error) error {
	// TODO: refactor later.
	return grpc.Errorf(codes.Unknown, err.Error())
}

func (s *RPCServer) header(err *pb.Error) *pb.ResponseHeader {
	return &pb.ResponseHeader{ClusterId: s.ClusterID(), Error: err}
}

func (s *RPCServer) tryGetRaftCluster() (*server.RaftCluster, *pb.Error) {
	cluster := s.GetRaftCluster()
	if cluster == nil {
		return nil, &pb.Error{
			Type:    pb.ErrorType_NOT_BOOTSTRAPPED,
			Message: "cluster is not bootstrapped",
		}
	}
	return cluster, nil
}

// checkStore returns an error response if the store exists and is in tombstone state.
// It returns nil if it can't get the store.
// Copied from server/command.go
func checkStore(cluster *server.RaftCluster, storeID uint64) *pb.Error {
	store, _, err := cluster.GetStore(storeID)
	if err == nil && store != nil {
		if store.GetState() == metapb.StoreState_Tombstone {
			return &pb.Error{
				Type:    pb.ErrorType_STORE_TOMBSTONE,
				Message: "store is tombstone",
			}
		}
	}
	return nil
}

// Server API for PD service

func (s *RPCServer) Tso(ctx context.Context, request *pb.TsoRequest) (*pb.TsoResponse, error) {
	count := request.GetCount()
	ts, err := s.GetRespTS(count)
	if err != nil {
		return nil, togRPCError(errors.Trace(err))
	}

	// Covert to pdpb2
	return &pb.TsoResponse{
		Header: s.header(nil),
		Count:  count,
		Timestamp: &pb.Timestamp{
			Physical: ts.Physical,
			Logical:  ts.Logical,
		},
	}, nil
}

func (s *RPCServer) Bootstrap(ctx context.Context, request *pb.BootstrapRequest) (*pb.BootstrapResponse, error) {
	cluster := s.GetRaftCluster()
	if cluster != nil {
		err := &pb.Error{
			Type:    pb.ErrorType_BOOTSTRAPPED,
			Message: "cluster is already bootstrapped",
		}
		return &pb.BootstrapResponse{
			Header: s.header(err),
		}, nil
	}

	return &pb.BootstrapResponse{
		Header: s.header(nil),
	}, nil
}

// TODO: Carry it by rpc Bootstrap? Since a BOOTSTRAPPED Error is already
// definded in ResponseHeader.
func (s *RPCServer) IsBootstrapped(ctx context.Context, request *pb.IsBootstrappedRequest) (*pb.IsBootstrappedResponse, error) {
	cluster := s.GetRaftCluster()

	return &pb.IsBootstrappedResponse{
		Header:       s.header(nil),
		Bootstrapped: cluster != nil,
	}, nil
}

func (s *RPCServer) AllocID(ctx context.Context, request *pb.AllocIDRequest) (*pb.AllocIDResponse, error) {
	// We can use an allocator for all types ID allocation.
	id, err := s.GetIDAllocator().Alloc()
	if err != nil {
		return nil, togRPCError(errors.Trace(err))
	}

	return &pb.AllocIDResponse{
		Header: s.header(nil),
		Id:     id,
	}, nil
}

func (s *RPCServer) GetStore(ctx context.Context, request *pb.GetStoreRequest) (*pb.GetStoreResponse, error) {
	cluster, pberr := s.tryGetRaftCluster()
	if pberr != nil {
		return &pb.GetStoreResponse{
			Header: s.header(pberr),
		}, nil
	}

	storeID := request.GetStoreId()
	store, _, err := cluster.GetStore(storeID)
	if err != nil {
		return nil, togRPCError(errors.Trace(err))
	}

	return &pb.GetStoreResponse{
		Header: s.header(nil),
		Store:  store,
	}, nil
}

func (s *RPCServer) PutStore(ctx context.Context, request *pb.PutStoreRequest) (*pb.PutStoreResponse, error) {
	cluster, pberr := s.tryGetRaftCluster()
	if pberr != nil {
		return &pb.PutStoreResponse{
			Header: s.header(pberr),
		}, nil
	}

	store := request.GetStore()
	if pberr := checkStore(cluster, store.GetId()); pberr != nil {
		return &pb.PutStoreResponse{
			Header: s.header(pberr),
		}, nil
	}

	if err := cluster.PutStore(store); err != nil {
		return nil, togRPCError(errors.Trace(err))
	}

	log.Infof("put store ok - %v", store)

	return &pb.PutStoreResponse{
		Header: s.header(nil),
	}, nil
}

func (s *RPCServer) StoreHeartbeat(svr pb.PD_StoreHeartbeatServer) error {
	for {
		req, err := svr.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		var stats *pdpb.StoreStats
		resp := &pb.StoreHeartbeatResponse{}
		cluster, pberr := s.tryGetRaftCluster()
		if pberr != nil {
			resp.Header = s.header(pberr)
			goto RESP
		}

		if pberr := checkStore(cluster, req.Stats.GetStoreId()); pberr != nil {
			resp.Header = s.header(pberr)
			goto RESP
		}

		stats = &pdpb.StoreStats{
			StoreId:            req.Stats.StoreId,
			Capacity:           req.Stats.Capacity,
			Available:          req.Stats.Available,
			RegionCount:        req.Stats.RegionCount,
			SendingSnapCount:   req.Stats.SendingSnapCount,
			ReceivingSnapCount: req.Stats.ReceivingSnapCount,
			StartTime:          req.Stats.StartTime,
			ApplyingSnapCount:  req.Stats.ApplyingSnapCount,
			IsBusy:             req.Stats.IsBusy,
		}
		err = cluster.StoreHeartbeat(stats)
		if err != nil {
			pberr := &pb.Error{
				Type:    pb.ErrorType_UNKNOWN,
				Message: errors.Trace(err).Error(),
			}
			resp.Header = s.header(pberr)
			goto RESP
		}

		resp = &pb.StoreHeartbeatResponse{Header: s.header(nil)}

	RESP:
		if err := svr.Send(resp); err != nil {
			return err
		}
	}
}

func (s *RPCServer) AskSplit(ctx context.Context, request *pb.AskSplitRequest) (*pb.AskSplitResponse, error) {
	cluster, pberr := s.tryGetRaftCluster()
	if pberr != nil {
		return &pb.AskSplitResponse{
			Header: s.header(pberr),
		}, nil
	}

	if request.GetRegion().GetStartKey() == nil {
		return nil, togRPCError(errors.New("missing region start key for split"))
	}

	req := &pdpb.AskSplitRequest{
		Region: request.Region,
	}
	split, err := cluster.HandleAskSplit(req)
	if err != nil {
		return nil, togRPCError(errors.Trace(err))
	}

	return &pb.AskSplitResponse{
		Header:      s.header(nil),
		NewRegionId: split.NewRegionId,
		NewPeerIds:  split.NewPeerIds,
	}, nil
}

func (s *RPCServer) ReportSplit(ctx context.Context, request *pb.ReportSplitRequest) (*pb.ReportSplitResponse, error) {
	cluster, pberr := s.tryGetRaftCluster()
	if pberr != nil {
		return &pb.ReportSplitResponse{
			Header: s.header(pberr),
		}, nil
	}

	req := &pdpb.ReportSplitRequest{
		Left:  request.Left,
		Right: request.Right,
	}
	_, err := cluster.HandleReportSplit(req)
	if err != nil {
		return nil, togRPCError(errors.Trace(err))
	}

	return &pb.ReportSplitResponse{
		Header: s.header(nil),
	}, nil
}

func (s *RPCServer) GetRegion(ctx context.Context, request *pb.GetRegionRequest) (*pb.GetRegionResponse, error) {
	cluster, pberr := s.tryGetRaftCluster()
	if pberr != nil {
		return &pb.GetRegionResponse{
			Header: s.header(pberr),
		}, nil
	}

	key := request.GetRegionKey()
	region, leader := cluster.GetRegion(key)
	return &pb.GetRegionResponse{
		Header: s.header(nil),
		Region: region,
		Leader: leader,
	}, nil
}

// TODO: Carry it by rpc GetRegion?
// Just a matter of a new filed in GetRegionRequest.
func (s *RPCServer) GetRegionByID(ctx context.Context, request *pb.GetRegionByIDRequest) (*pb.GetRegionResponse, error) {
	cluster, pberr := s.tryGetRaftCluster()
	if pberr != nil {
		return &pb.GetRegionResponse{
			Header: s.header(pberr),
		}, nil
	}

	id := request.GetRegionId()
	region, leader := cluster.GetRegionByID(id)
	return &pb.GetRegionResponse{
		Header: s.header(nil),
		Region: region,
		Leader: leader,
	}, nil
}

func (s *RPCServer) RegionHeartbeat(svr pb.PD_RegionHeartbeatServer) error {
	// 	for {
	// 		req, err := svr.Recv()
	// 		if err == io.EOF {
	// 			return nil
	// 		}
	// 		if err != nil {
	// 			return err
	// 		}

	// 		cluster, pberr := s.tryGetRaftCluster()
	// 		if pberr != nil {
	// 			return &pb.PutStoreResponse{
	// 				Header: s.header(pberr),
	// 			}, nil
	// 		}

	// 		if pberr := checkStore(cluster, store.GetId()); pberr != nil {
	// 			return &pb.PutStoreResponse{
	// 				Header: s.header(pberr),
	// 			}, nil
	// 		}

	// 		region := newRegionInfo(request.GetRegion(), request.GetLeader())
	// 		region.DownPeers = request.GetDownPeers()
	// 		region.PendingPeers = request.GetPendingPeers()
	// 		if region.GetId() == 0 {
	// 			return nil, errors.Errorf("invalid request region, %v", request)
	// 		}
	// 		if region.Leader == nil {
	// 			return nil, errors.Errorf("invalid request leader, %v", request)
	// 		}

	// 		err = cluster.cachedCluster.handleRegionHeartbeat(region)
	// 		if err != nil {
	// 			return nil, errors.Trace(err)
	// 		}

	// 		res, err := cluster.handleRegionHeartbeat(region)
	// 		if err != nil {
	// 			return nil, errors.Trace(err)
	// 		}

	// 		if err != nil {
	// 			return nil, togRPCError(errors.Trace(err))
	// 		}

	// 		resp := &pb.RegionHeartbeatResponse{
	// 			Header: s.header(nil),
	// 	ChangePeer: res.ChangePeer
	// //
	// TransferLeader:
	// 	}
	// 		if err := svr.Send(resp); err != nil {
	// 			return err
	// 		}
	// 	}
	return togRPCError(errors.Errorf("not impl"))
}

func (s *RPCServer) GetClusterConfig(ctx context.Context, request *pb.GetClusterConfigRequest) (*pb.GetClusterConfigResponse, error) {
	cluster, pberr := s.tryGetRaftCluster()
	if pberr != nil {
		return &pb.GetClusterConfigResponse{
			Header: s.header(pberr),
		}, nil
	}

	conf := cluster.GetConfig()
	return &pb.GetClusterConfigResponse{
		Header:  s.header(nil),
		Cluster: conf,
	}, nil
}

func (s *RPCServer) PutClusterConfig(ctx context.Context, request *pb.PutClusterConfigRequest) (*pb.PutClusterConfigResponse, error) {
	cluster, pberr := s.tryGetRaftCluster()
	if pberr != nil {
		return &pb.PutClusterConfigResponse{
			Header: s.header(pberr),
		}, nil
	}

	conf := request.GetCluster()
	if err := cluster.PutConfig(conf); err != nil {
		return nil, togRPCError(errors.Trace(err))
	}

	log.Infof("put cluster config ok - %v", conf)

	return &pb.PutClusterConfigResponse{
		Header: s.header(nil),
	}, nil
}

func (s *RPCServer) GetPDMembers(ctx context.Context, request *pb.GetPDMembersRequest) (*pb.GetPDMembersResponse, error) {
	client := s.GetClient()
	ms, err := server.GetPDMembers(client)
	if err != nil {
		return nil, togRPCError(errors.Trace(err))
	}

	members := make([]*pb.Member, 0, len(ms))
	for _, m := range ms {
		member := &pb.Member{
			Name:       *m.Name,
			MemberId:   s.ClusterID(),
			PeerUrls:   m.PeerUrls,
			ClientUrls: m.ClientUrls,
		}
		members = append(members, member)
	}

	return &pb.GetPDMembersResponse{
		Header:  s.header(nil),
		Members: members,
	}, nil
}
