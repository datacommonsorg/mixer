// Copyright 2023 Google LLC
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
	"sort"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/cache"
	"github.com/datacommonsorg/mixer/internal/server/statvar"
	"github.com/datacommonsorg/mixer/internal/store"
	"golang.org/x/sync/errgroup"
)

// VariableGroupInfo implements API for Mixer.VariableGroupInfo.
func VariableGroupInfo(
	ctx context.Context,
	in *pbv1.VariableGroupInfoRequest,
	store *store.Store,
	cachedata *cache.Cache,
) (*pbv1.VariableGroupInfoResponse, error) {
	data, err := statvar.GetStatVarGroupNode(
		ctx,
		&pb.GetStatVarGroupNodeRequest{
			StatVarGroup:         in.GetNode(),
			Entities:             in.GetConstrainedEntities(),
			NumEntitiesExistence: in.GetNumEntitiesExistence(),
		},
		store,
		cachedata,
	)
	if err != nil {
		return nil, err
	}
	return &pbv1.VariableGroupInfoResponse{Node: in.GetNode(), Info: data}, nil
}

// BulkVariableGroupInfo implements API for Mixer.BulkVariableGroupInfo.
func BulkVariableGroupInfo(
	ctx context.Context,
	in *pbv1.BulkVariableGroupInfoRequest,
	store *store.Store,
	cachedata *cache.Cache,
) (*pbv1.BulkVariableGroupInfoResponse, error) {
	// TODO (shifucun):
	// Ideally, both APIs need to filter out the child variable (group) that has
	// no data, but this is indicated with a "has_data" field, to
	// accomocate the "Show all statistical variables" in UI widget. The UI
	// should call this API twice, w/o constrained_entities to achieve that.
	nodes := in.GetNodes()
	constraindEntities := in.GetConstrainedEntities()
	numEntitiesExistence := in.GetNumEntitiesExistence()

	if len(nodes) == 0 {
		result := &pbv1.BulkVariableGroupInfoResponse{
			Data: []*pbv1.VariableGroupInfoResponse{},
		}
		rawSvgs := cachedata.RawSvgs(ctx)
		for svg := range rawSvgs {
			result.Data = append(result.Data, &pbv1.VariableGroupInfoResponse{
				Node: svg,
				Info: rawSvgs[svg],
			})
		}
		sort.SliceStable(result.Data, func(i, j int) bool {
			return result.Data[i].Node < result.Data[j].Node
		})
		return result, nil
	}

	dataChan := make(chan *pbv1.VariableGroupInfoResponse, len(nodes))
	errs, errCtx := errgroup.WithContext(ctx)
	for _, node := range nodes {
		node := node
		errs.Go(func() error {
			data, err := VariableGroupInfo(
				errCtx,
				&pbv1.VariableGroupInfoRequest{
					Node:                 node,
					ConstrainedEntities:  constraindEntities,
					NumEntitiesExistence: numEntitiesExistence,
				},
				store,
				cachedata,
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
	resp := &pbv1.BulkVariableGroupInfoResponse{
		Data: []*pbv1.VariableGroupInfoResponse{},
	}
	for elem := range dataChan {
		resp.Data = append(resp.Data, elem)
	}

	sort.Slice(resp.Data, func(i, j int) bool {
		return resp.Data[i].Node < resp.Data[j].Node
	})
	return resp, nil
}
