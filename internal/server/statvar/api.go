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

	rawSvgs := cachedata.RawSvgs(ctx)
	result := &pb.StatVarGroupNode{}
	if r, ok := rawSvgs[svg]; ok {
		// Clone into result, otherwise the server cache is modified.
		result = proto.Clone(r).(*pb.StatVarGroupNode)
	}
	for _, item := range result.ChildStatVarGroups {
		item.DisplayName = rawSvgs[item.Id].AbsoluteName
		item.DescendentStatVarCount = rawSvgs[item.Id].DescendentStatVarCount
	}
	for _, item := range result.ChildStatVars {
		item.HasData = true
	}
	result.ParentStatVarGroups = cachedata.ParentSvgs(ctx)[svg]

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
	result := &pb.StatVarGroups{StatVarGroups: cachedata.RawSvgs(ctx)}
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
