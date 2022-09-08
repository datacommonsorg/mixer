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
)

// VariableGroupInfo implements API for Mixer.VariableGroupInfo.
func VariableGroupInfo(
	ctx context.Context,
	in *pb.VariableGroupInfoRequest,
	store *store.Store,
	cache *resource.Cache,
) (*pb.VariableGroupInfoResponse, error) {
	data, err := statvar.GetStatVarGroupNode(
		ctx,
		&pb.GetStatVarGroupNodeRequest{
			StatVarGroup: in.GetNode(),
			Entities:     in.GetConstrainedEntities(),
		},
		store,
		cache,
	)
	if err != nil {
		return nil, err
	}
	return &pb.VariableGroupInfoResponse{Node: in.GetNode(), Info: data}, nil
}

// BulkVariableGroupInfo implements API for Mixer.BulkVariableGroupInfo.
func BulkVariableGroupInfo(
	ctx context.Context,
	in *pb.BulkVariableGroupInfoRequest,
	store *store.Store,
	cache *resource.Cache,
) (*pb.BulkVariableGroupInfoResponse, error) {
	// TODO(shifucun): handle "nodes" in request, now always return all the
	// varible group data.
	entities := in.GetConstrainedEntities()
	tmp, err := statvar.GetStatVarGroup(
		ctx,
		&pb.GetStatVarGroupRequest{Entities: entities},
		store,
		cache,
	)
	if err != nil {
		return nil, err
	}
	resp := &pb.BulkVariableGroupInfoResponse{Data: []*pb.VariableGroupInfoResponse{}}
	for node, info := range tmp.StatVarGroups {
		resp.Data = append(resp.Data, &pb.VariableGroupInfoResponse{
			Node: node,
			Info: info,
		})
	}
	return resp, nil
}
