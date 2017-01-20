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

package v1rpc

import "github.com/pingcap/pd/server"

// RPCServer provide v1rpc services.
type RPCServer struct {
	*server.Server
}

// NewRPCServer create a new RPCServer.
func NewRPCServer(svr *server.Server) *RPCServer {
	return *RPCServer{
		Server: svr,
	}
}
