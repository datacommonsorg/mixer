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

package statvar

import (
	"context"
	"sort"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/cache"
	"github.com/datacommonsorg/mixer/internal/server/count"
	"github.com/datacommonsorg/mixer/internal/server/statvar/fetcher"
	"github.com/datacommonsorg/mixer/internal/server/statvar/hierarchy"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// GetPlaceStatsVar implements API for Mixer.GetPlaceStatsVar.
// TODO(shifucun): Migrate clients to use GetPlaceStatVars and deprecate this.
func GetPlaceStatsVar(
	ctx context.Context,
	in *pb.GetPlaceStatsVarRequest,
	store *store.Store,
) (
	*pb.GetPlaceStatsVarResponse, error,
) {
	dcids := in.GetDcids()
	if len(dcids) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Missing required arguments: dcids")
	}
	if err := util.CheckValidDCIDs(dcids); err != nil {
		return nil, err
	}

	resp, err := fetcher.FetchEntityVariables(ctx, store, dcids)
	if err != nil {
		return nil, err
	}
	out := pb.GetPlaceStatsVarResponse{Places: map[string]*pb.StatsVars{}}
	for dcid, statVars := range resp {
		out.Places[dcid] = &pb.StatsVars{StatsVars: statVars.StatVars}
	}
	return &out, nil
}

// GetEntityStatVarsUnionV1 implements API for Mixer.GetEntityStatVarsUnionV1.
func GetEntityStatVarsUnionV1(
	ctx context.Context,
	in *pb.GetEntityStatVarsUnionRequest,
	store *store.Store,
	cachedata *cache.Cache,
) (*pb.GetEntityStatVarsUnionResponse, error) {
	// Check entities
	entities := in.GetDcids()
	if len(entities) == 0 {
		return nil, status.Error(
			codes.InvalidArgument, "Missing required arguments: dcids")
	}
	// filtered stat vars
	filterStatVars := in.GetStatVars()
	// Create a set to make the loop up logic more efficient
	filterStatVarSet := map[string]struct{}{}
	for _, sv := range filterStatVars {
		filterStatVarSet[sv] = struct{}{}
	}
	result := &pb.GetEntityStatVarsUnionResponse{}

	// When given a list of stat vars to filter for, we can use the existence
	// cache instead to check the existence of each stat var for the list of
	// entities. This is faster than getting all the stat vars for each entity and
	// then filtering.
	if len(filterStatVars) > 0 && len(entities) > 0 {
		statVarCount, err := count.Count(ctx, store, cachedata, filterStatVars, entities)
		if err != nil {
			return nil, err
		}
		for sv := range filterStatVarSet {
			if existence, ok := statVarCount[sv]; ok && len(existence) > 0 {
				result.StatVars = append(result.StatVars, sv)
			}
		}
	} else {
		resp, err := fetcher.FetchEntityVariables(ctx, store, entities)
		if err != nil {
			return nil, err
		}
		place2StatVars := resp

		// For single entity, return directly.
		if len(entities) == 1 {
			return &pb.GetEntityStatVarsUnionResponse{StatVars: place2StatVars[entities[0]].StatVars}, nil
		}

		// Get union of the statvars for multiple entities.
		set := map[string]bool{}
		for _, statVars := range place2StatVars {
			for _, sv := range statVars.GetStatVars() {
				set[sv] = true
			}
		}
		result.StatVars = util.KeysToSlice(set)
	}
	return result, nil
}

// GetStatVarGroupNode implements API for Mixer.GetStatVarGroupNode.
func GetStatVarGroupNode(
	ctx context.Context,
	in *pb.GetStatVarGroupNodeRequest,
	store *store.Store,
	cachedata *cache.Cache,
) (*pb.StatVarGroupNode, error) {
	entities := in.GetEntities()
	svg := in.GetStatVarGroup()
	numEntitiesExistence := int(in.GetNumEntitiesExistence())
	// We want at least 1 entity to have data.
	if numEntitiesExistence == 0 {
		numEntitiesExistence = 1
	}

	if svg == "" {
		return nil, status.Errorf(
			codes.InvalidArgument, "Missing required argument: stat_var_group")
	}

	result := &pb.StatVarGroupNode{}
	if r, ok := cachedata.RawSvgs()[svg]; ok {
		// Clone into result, otherwise the server cache is modified.
		result = proto.Clone(r).(*pb.StatVarGroupNode)
	}
	for _, item := range result.ChildStatVarGroups {
		item.DisplayName = cachedata.RawSvgs()[item.Id].AbsoluteName
		item.DescendentStatVarCount = cachedata.RawSvgs()[item.Id].DescendentStatVarCount
	}
	for _, item := range result.ChildStatVars {
		item.HasData = true
	}
	result.ParentStatVarGroups = cachedata.ParentSvgs()[svg]

	// Filter result based on entities
	if len(entities) > 0 {
		// Get the stat var and stat var group IDs to check if they are valid for
		// given entities.
		allIDs := []string{svg}
		for _, item := range result.ChildStatVarGroups {
			allIDs = append(allIDs, item.Id)
		}
		for _, item := range result.ChildStatVars {
			allIDs = append(allIDs, item.Id)
		}
		// Check if stat data exists for given entities
		statVarCount, err := count.Count(ctx, store, cachedata, allIDs, entities)
		if err != nil {
			return nil, err
		}
		// Count for current node.
		result.DescendentStatVarCount = 0
		if existence, ok := statVarCount[svg]; ok && len(existence) >= numEntitiesExistence {
			counts := []int32{}
			for _, count := range existence {
				counts = append(counts, count)
			}
			sort.Slice(counts, func(i, j int) bool { return counts[i] > counts[j] })
			// Use the numEntitiesExistence-th largest count
			result.DescendentStatVarCount = counts[numEntitiesExistence-1]
		}
		// Filter child stat var groups
		for _, item := range result.ChildStatVarGroups {
			item.DescendentStatVarCount = 0
			if existence, ok := statVarCount[item.Id]; ok && len(existence) >= numEntitiesExistence {
				counts := []int32{}
				for _, count := range existence {
					counts = append(counts, count)
				}
				sort.Slice(counts, func(i, j int) bool { return counts[i] > counts[j] })
				// Use the numEntitiesExistence-th largest count
				item.DescendentStatVarCount = counts[numEntitiesExistence-1]
			}
		}
		// Filter child stat vars
		for _, item := range result.ChildStatVars {
			if existence, ok := statVarCount[item.Id]; !ok || len(existence) < numEntitiesExistence {
				item.HasData = false
			}
		}
	}
	return result, nil
}

// GetStatVarGroup implements API for Mixer.GetStatVarGroup.
func GetStatVarGroup(
	ctx context.Context,
	in *pb.GetStatVarGroupRequest,
	store *store.Store,
	cachedata *cache.Cache,
) (*pb.StatVarGroups, error) {
	defer util.TimeTrack(time.Now(), "GetStatVarGroup")
	result := &pb.StatVarGroups{StatVarGroups: cachedata.RawSvgs()}
	// Only read entity stat vars when entities are provided.
	// User can provide any arbitrary dcid, which might not be associated with
	// stat vars. In this case, an empty response is returned.
	entities := in.GetEntities()
	if len(entities) > 0 {
		var statVars []string
		entity2variables, err := fetcher.FetchEntityVariables(ctx, store, entities)
		if err != nil {
			return nil, err
		}
		for _, sv := range entity2variables {
			statVars = util.MergeDedupe(statVars, sv.StatVars)
		}
		// FilterSVG makes a copy of result.StatVarGroups, so cachedata.RawSvg is
		// un-modified.
		result.StatVarGroups = hierarchy.FilterSVG(result.StatVarGroups, statVars)
	}
	return result, nil
}
