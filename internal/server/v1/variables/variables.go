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

package variables

import (
	"context"
	"sort"

	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/statvar/fetcher"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/datacommonsorg/mixer/internal/store"
)

// Variables implements API for Mixer.Variables.
func Variables(
	ctx context.Context,
	in *pbv1.VariablesRequest,
	store *store.Store,
) (*pbv1.VariablesResponse, error) {
	entity := in.GetEntity()
	if err := util.CheckValidDCIDs([]string{entity}); err != nil {
		return nil, err
	}
	entityToStatVars, err := fetcher.FetchEntityVariables(ctx, store, []string{entity})
	if err != nil {
		return nil, err
	}

	resp := &pbv1.VariablesResponse{Entity: entity}
	statVars, ok := entityToStatVars[entity]
	if !ok {
		return resp, nil
	}
	resp.Variables = statVars.StatVars

	return resp, nil
}

// BulkVariables implements API for Mixer.BulkVariables.
func BulkVariables(
	ctx context.Context,
	in *pbv1.BulkVariablesRequest,
	store *store.Store,
) (*pbv1.BulkVariablesResponse, error) {
	entities := in.GetEntities()
	if len(entities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Missing required arguments: entities")
	}
	if err := util.CheckValidDCIDs(entities); err != nil {
		return nil, err
	}
	entityToStatVars, err := fetcher.FetchEntityVariables(ctx, store, entities)
	if err != nil {
		return nil, err
	}

	resp := &pbv1.BulkVariablesResponse{
		Data: []*pbv1.VariablesResponse{},
	}

	if in.GetUnion() {
		statVarSet := map[string]struct{}{}
		for _, statVars := range entityToStatVars {
			for _, statVar := range statVars.StatVars {
				statVarSet[statVar] = struct{}{}
			}
		}
		resp.Data = append(resp.Data, &pbv1.VariablesResponse{})
		for statVar := range statVarSet {
			resp.Data[0].Variables = append(resp.Data[0].Variables, statVar)
		}
		sort.Strings(resp.Data[0].Variables)
	} else {
		sort.Strings(entities)
		for _, entity := range entities {
			item := &pbv1.VariablesResponse{
				Entity: entity,
			}
			if statVars, ok := entityToStatVars[entity]; ok {
				item.Variables = statVars.StatVars
			}
			resp.Data = append(resp.Data, item)
		}
	}

	return resp, nil
}
