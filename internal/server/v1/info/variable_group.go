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
	"golang.org/x/sync/errgroup"
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
	// TODO (shifucun):
	// Ideally, both APIs need to filter out the child variable (group) that has
	// no data, but this is indicated with a "has_data" field, to
	// accomocate the "Show all statistical variables" in UI widget. The UI
	// should call this API twice, w/o constrained_entities to achieve that.
	nodes := in.GetNodes()
	constraindEntities := in.GetConstrainedEntities()
	dataChan := make(chan *pb.VariableGroupInfoResponse, len(nodes))
	errs, errCtx := errgroup.WithContext(ctx)
	for _, node := range nodes {
		node := node
		errs.Go(func() error {
			data, err := VariableGroupInfo(
				errCtx,
				&pb.VariableGroupInfoRequest{
					Node:                node,
					ConstrainedEntities: constraindEntities,
				},
				store,
				cache,
			)
			dataChan <- data
			return err
		})
	}
	err := errs.Wait()
	if err != nil {
		return nil, err
	}
	close(dataChan)
	resp := &pb.BulkVariableGroupInfoResponse{
		Data: []*pb.VariableGroupInfoResponse{},
	}
	for elem := range dataChan {
		resp.Data = append(resp.Data, elem)
	}

	return resp, nil
}
