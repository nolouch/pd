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
	"github.com/juju/errors"
	pb "github.com/pingcap/kvproto/pkg/pdpb2"
	"github.com/pingcap/pd/server"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

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

func (s *RPCServer) header() *pb.ResponseHeader {
	return &pb.ResponseHeader{ClusterId: s.ClusterID()}
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
		Header: s.header(),
		Count:  count,
		Timestamp: &pb.Timestamp{
			Physical: ts.Physical,
			Logical:  ts.Logical,
		},
	}, nil
}

func (s *RPCServer) Bootstrap(ctx context.Context, request *pb.BootstrapRequest) (*pb.BootstrapResponse, error) {
	cluster := s.GetRaftCluster()
	header := s.header()
	if cluster != nil {
		header.Error = &pb.Error{
			Type:    pb.ErrorType_BOOTSTRAPPED,
			Message: "cluster is already bootstrapped",
		}
	}

	return &pb.BootstrapResponse{
		Header: header,
	}, nil
}

// TODO: Carry it by rpc Bootstrap? Since a BOOTSTRAPPED Error is already
// definded in ResponseHeader.
func (s *RPCServer) IsBootstrapped(ctx context.Context, request *pb.IsBootstrappedRequest) (*pb.IsBootstrappedResponse, error) {
	cluster := s.GetRaftCluster()

	return &pb.IsBootstrappedResponse{
		Header:       s.header(),
		Bootstrapped: cluster != nil,
	}, nil
}

func (s *RPCServer) AllocId(ctx context.Context, request *pb.AllocIdRequest) (*pb.AllocIdResponse, error) {
	return nil, togRPCError(errors.Errorf("not impl"))

}
func (s *RPCServer) GetStore(ctx context.Context, request *pb.GetStoreRequest) (*pb.GetStoreResponse, error) {
	return nil, togRPCError(errors.Errorf("not impl"))

}
func (s *RPCServer) PutStore(ctx context.Context, request *pb.PutStoreRequest) (*pb.PutStoreResponse, error) {
	return nil, togRPCError(errors.Errorf("not impl"))

}
func (s *RPCServer) StoreHeartbeat(svr pb.PD_StoreHeartbeatServer) error {
	return togRPCError(errors.Errorf("not impl"))
}

func (s *RPCServer) AskSplit(ctx context.Context, request *pb.AskSplitRequest) (*pb.AskSplitResponse, error) {
	return nil, togRPCError(errors.Errorf("not impl"))

}
func (s *RPCServer) ReportSplit(ctx context.Context, request *pb.ReportSplitRequest) (*pb.ReportSplitResponse, error) {
	return nil, togRPCError(errors.Errorf("not impl"))
}

func (s *RPCServer) GetRegion(ctx context.Context, request *pb.GetRegionRequest) (*pb.GetStoreResponse, error) {
	return nil, togRPCError(errors.Errorf("not impl"))
}

// TODO: Carry it by rpc GetRegion?
// Just a matter of a new filed in GetRegionRequest.
func (s *RPCServer) GetRegionByID(ctx context.Context, request *pb.GetRegionByIDRequest) (*pb.GetRegionResponse, error) {
	return nil, togRPCError(errors.Errorf("not impl"))
}

func (s *RPCServer) RegionHeartbeat(ctx context.Context, request *pb.RegionHeartbeatRequest) (*pb.RegionHeartbeatResponse, error) {
	return nil, togRPCError(errors.Errorf("not impl"))
}

func (s *RPCServer) GetClusterConfig(ctx context.Context, request *pb.GetClusterConfigRequest) (*pb.GetClusterConfigResponse, error) {
	return nil, togRPCError(errors.Errorf("not impl"))
}

func (s *RPCServer) PutClusterConfig(ctx context.Context, request *pb.PutClusterConfigRequest) (*pb.PutClusterConfigResponse, error) {
	return nil, togRPCError(errors.Errorf("not impl"))
}

func (s *RPCServer) GetPDMembers(ctx context.Context, request *pb.GetPDMembersRequest) (*pb.GetPDMembersResponse, error) {
	return nil, togRPCError(errors.Errorf("not impl"))
}
