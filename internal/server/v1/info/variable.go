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
	"github.com/datacommonsorg/mixer/internal/server/resource"
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
	entity := in.GetEntity()
	if !util.CheckValidDCIDs([]string{entity}) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid entity")
	}
	statVarToSummary, err := statvar.GetStatVarSummaryHelper(ctx, []string{entity}, store)
	if err != nil {
		return nil, err
	}
	resp := &pb.VariableInfoResponse{Entity: entity}
	if summary, ok := statVarToSummary[entity]; ok {
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
	entities := in.GetEntities()
	if len(entities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Missing required arguments: entities")
	}
	if !util.CheckValidDCIDs(entities) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid entities")
	}
	statVarToSummary, err := statvar.GetStatVarSummaryHelper(ctx, entities, store)
	if err != nil {
		return nil, err
	}
	resp := &pb.BulkVariableInfoResponse{}
	for _, entity := range entities {
		item := &pb.VariableInfoResponse{Entity: entity}
		if summary, ok := statVarToSummary[entity]; ok {
			item.Info = summary
		}
		resp.Data = append(resp.Data, item)
	}
	return resp, nil
}

// VariableGroupInfo implements API for Mixer.VariableGroupInfo.
func VariableGroupInfo(
	ctx context.Context,
	in *pb.VariableGroupInfoRequest,
	store *store.Store,
	cache *resource.Cache,
) (*pb.StatVarGroupNode, error) {
	return statvar.GetStatVarGroupNode(
		ctx,
		&pb.GetStatVarGroupNodeRequest{
			StatVarGroup: in.GetDcid(),
			Entities:     in.GetEntities(),
		},
		store,
		cache,
	)
}
