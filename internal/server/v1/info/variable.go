// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package info

import (
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/statvar"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// VariableInfo implements API for Mixer.VariableInfo.
func VariableInfo(
	ctx context.Context,
	in *pb.VariableInfoRequest,
	store *store.Store,
) (*pb.VariableInfoResponse, error) {
	node := in.GetNode()
	if !util.CheckValidDCIDs([]string{node}) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid node")
	}
	statVarToSummary, err := statvar.GetStatVarSummaryHelper(ctx, []string{node}, store)
	if err != nil {
		return nil, err
	}
	resp := &pb.VariableInfoResponse{Node: node}
	if summary, ok := statVarToSummary[node]; ok {
		resp.Info = summary
	}
	return resp, nil
}

// BulkVariableInfo implements API for Mixer.BulkVariableInfo.
func BulkVariableInfo(
	ctx context.Context,
	in *pb.BulkVariableInfoRequest,
	store *store.Store,
) (*pb.BulkVariableInfoResponse, error) {
	nodes := in.GetNodes()
	if len(nodes) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Missing required arguments: nodes")
	}
	if !util.CheckValidDCIDs(nodes) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid nodes")
	}
	statVarToSummary, err := statvar.GetStatVarSummaryHelper(ctx, nodes, store)
	if err != nil {
		return nil, err
	}
	resp := &pb.BulkVariableInfoResponse{}
	for _, node := range nodes {
		item := &pb.VariableInfoResponse{Node: node}
		if summary, ok := statVarToSummary[node]; ok {
			item.Info = summary
		}
		resp.Data = append(resp.Data, item)
	}
	return resp, nil
}
