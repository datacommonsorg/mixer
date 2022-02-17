// Copyright 2021 Google LLC
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

	cbt "cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	svgRoot = "dc/g/Root"
)

// Note this function modifies validSVG inside.
func markValidSVG(
	svgResp *pb.StatVarGroups, svgID string, validSVG map[string]struct{}) bool {
	// Already checked
	if _, ok := validSVG[svgID]; ok {
		return true
	}
	svChildren := svgResp.StatVarGroups[svgID].ChildStatVars
	svgChildren := svgResp.StatVarGroups[svgID].ChildStatVarGroups
	// If there are non-empty sv chldren, then this svg is valid
	if len(svChildren) > 0 {
		validSVG[svgID] = struct{}{}
		return true
	}
	// Recursively check child svg, if there is any valid svg child, then this
	// is valid too
	for _, svgChild := range svgChildren {
		if markValidSVG(svgResp, svgChild.Id, validSVG) {
			validSVG[svgID] = struct{}{}
			return true
		}
	}
	return false
}

// Filter StatVarGroups based on give stat vars. This does not modify the input
// data but create a filtered copy of it.
func filterSVG(in *pb.StatVarGroups, statVars []string) *pb.StatVarGroups {
	out := &pb.StatVarGroups{StatVarGroups: map[string]*pb.StatVarGroupNode{}}
	// Build set for all the SV.
	validSV := map[string]struct{}{}
	for _, sv := range statVars {
		validSV[sv] = struct{}{}
	}

	// Step 1: iterate over stat var group, and only keep stat var children with valid
	// stat vars for this place.
	for sv, svgData := range in.StatVarGroups {
		filteredChildren := []*pb.StatVarGroupNode_ChildSV{}
		for _, child := range svgData.ChildStatVars {
			if _, ok := validSV[child.Id]; ok {
				filteredChildren = append(filteredChildren, child)
			}
		}
		out.StatVarGroups[sv] = &pb.StatVarGroupNode{
			ChildStatVars:      filteredChildren,
			ChildStatVarGroups: svgData.ChildStatVarGroups,
		}
	}
	// Step 2: recursively check if a stat var group is valid. A stat var group
	// is valid if it has any descendent stat var group with non-empty stat vars

	// All the svg with valid sv for this place
	validSVG := map[string]struct{}{}
	for svgID := range out.StatVarGroups {
		markValidSVG(out, svgID, validSVG)
	}

	// Step3: another iteration to only keep valid svg
	for svgID, svgData := range out.StatVarGroups {
		filteredChildren := []*pb.StatVarGroupNode_ChildSVG{}
		for _, c := range svgData.ChildStatVarGroups {
			if _, ok := validSVG[c.Id]; ok {
				filteredChildren = append(filteredChildren, c)
			}
		}
		out.StatVarGroups[svgID].ChildStatVarGroups = filteredChildren
		d := out.StatVarGroups[svgID]
		if len(d.ChildStatVars) == 0 && len(d.ChildStatVarGroups) == 0 {
			delete(out.StatVarGroups, svgID)
		}
	}
	return out
}

// GetStatVarGroup implements API for Mixer.GetStatVarGroup.
func GetStatVarGroup(
	ctx context.Context,
	in *pb.GetStatVarGroupRequest,
	store *store.Store,
	cache *resource.Cache,
) (
	*pb.StatVarGroups, error) {
	places := in.GetPlaces()

	var statVars []string
	// Only read place stat vars when the place is provided.
	// User can provide any arbitrary dcid, which might not be associated with
	// stat vars. In this case, an empty response is returned.
	if len(places) > 0 {
		svUnionResp, err := GetPlaceStatVarsUnionV1(
			ctx,
			&pb.GetPlaceStatVarsUnionRequest{Dcids: places},
			store,
		)
		if err != nil {
			return nil, err
		}
		statVars = svUnionResp.StatVars
	}

	var result *pb.StatVarGroups
	if cache == nil {
		// Read stat var group cache from the first table, which is the most
		// preferred cache BigTable. Since stat var schemas are rebuild with every
		// group, so no merge needed.
		btDataList, err := bigtable.Read(
			ctx,
			store.BtGroup,
			cbt.RowList{bigtable.BtStatVarGroup},
			func(dcid string, jsonRaw []byte, isProto bool) (interface{}, error) {
				var svgResp pb.StatVarGroups
				if isProto {
					if err := proto.Unmarshal(jsonRaw, &svgResp); err != nil {
						return nil, err
					}
				} else {
					if err := protojson.Unmarshal(jsonRaw, &svgResp); err != nil {
						return nil, err
					}
				}
				return &svgResp, nil
			},
			// Since there is no dcid, use "_" as a dummy token
			func(token string) (string, error) { return "_", nil },
		)
		if err != nil {
			return nil, err
		}
		result = btDataList[0]["_"].(*pb.StatVarGroups)
	} else {
		result = &pb.StatVarGroups{StatVarGroups: cache.RawSvg}
	}
	if len(places) > 0 {
		result = filterSVG(result, statVars)
	}
	return result, nil
}

// GetStatVarGroupNode implements API for Mixer.GetStatVarGroupNode.
func GetStatVarGroupNode(
	ctx context.Context,
	in *pb.GetStatVarGroupNodeRequest,
	store *store.Store,
	cache *resource.Cache,
) (*pb.StatVarGroupNode, error) {
	places := in.GetPlaces()
	svg := in.GetStatVarGroup()

	if svg == "" {
		return nil, status.Errorf(
			codes.InvalidArgument, "Missing required argument: stat_var_group")
	}

	result := &pb.StatVarGroupNode{}
	if r, ok := cache.RawSvg[svg]; ok {
		// Clone into result, otherwise the server cache is modified.
		result = proto.Clone(r).(*pb.StatVarGroupNode)
	}
	for _, item := range result.ChildStatVarGroups {
		item.DisplayName = cache.RawSvg[item.Id].AbsoluteName
		item.DescendentStatVarCount = cache.RawSvg[item.Id].DescendentStatVarCount
	}
	for _, item := range result.ChildStatVars {
		item.HasData = true
	}
	result.ParentStatVarGroups = cache.ParentSvg[svg]

	// Filter result based on places
	if len(places) > 0 {
		// Get the stat var and stat var group IDs to check if they are valid for
		// given places.
		allIDs := []string{svg}
		for _, item := range result.ChildStatVarGroups {
			allIDs = append(allIDs, item.Id)
		}
		for _, item := range result.ChildStatVars {
			allIDs = append(allIDs, item.Id)
		}
		allIDs = append(allIDs, result.ParentStatVarGroups...)
		// Check if stat data exists for given places
		statVarCount, err := Count(ctx, store.BtGroup, allIDs, places)
		if err != nil {
			return nil, err
		}
		// Count for current node.
		result.DescendentStatVarCount = 0
		if existence, ok := statVarCount[svg]; ok && len(existence) > 0 {
			for _, count := range existence {
				// Use the largest count among all places.
				if count > result.GetDescendentStatVarCount() {
					result.DescendentStatVarCount = count
				}
			}
		}
		// Filter child stat var groups
		for _, item := range result.ChildStatVarGroups {
			item.DescendentStatVarCount = 0
			if existence, ok := statVarCount[item.Id]; ok && len(existence) > 0 {
				for _, count := range existence {
					// Use the largest count among all places
					if count > item.DescendentStatVarCount {
						item.DescendentStatVarCount = count
					}
				}
			}
		}
		// Filter child stat vars
		for _, item := range result.ChildStatVars {
			if existence, ok := statVarCount[item.Id]; !ok || len(existence) == 0 {
				item.HasData = false
			}
		}
	}

	// Gather stat vars from the private import
	if !store.MemDb.IsEmpty() {
		hasDataStatVars, noDataStatVars := store.MemDb.GetStatVars([]string{})
		if svg == "dc/g/Root" {
			result.ChildStatVarGroups = append(
				result.ChildStatVarGroups,
				&pb.StatVarGroupNode_ChildSVG{
					Id:                     "dc/g/Private",
					SpecializedEntity:      store.MemDb.GetManifest().ImportName,
					DisplayName:            store.MemDb.GetManifest().ImportName,
					DescendentStatVarCount: int32(len(hasDataStatVars) + len(noDataStatVars)),
				},
			)
		} else if svg == "dc/g/Private" {
			for _, statVar := range hasDataStatVars {
				result.ChildStatVars = append(
					result.ChildStatVars,
					&pb.StatVarGroupNode_ChildSV{
						Id:          statVar,
						DisplayName: statVar,
						HasData:     true,
					},
				)
			}
			for _, statVar := range noDataStatVars {
				result.ChildStatVars = append(
					result.ChildStatVars,
					&pb.StatVarGroupNode_ChildSV{
						Id:          statVar,
						DisplayName: statVar,
						HasData:     false,
					},
				)
			}
		}
	}
	return result, nil
}

// GetStatVarPath implements API for Mixer.GetStatVarPath.
func GetStatVarPath(
	ctx context.Context,
	in *pb.GetStatVarPathRequest,
	store *store.Store,
	cache *resource.Cache,
) (
	*pb.GetStatVarPathResponse, error) {
	id := in.GetId()
	if id == "" {
		return nil, status.Errorf(
			codes.InvalidArgument, "Missing required argument: id")
	}
	// Memory database stat vars are directly under "dc/g/Private"
	if store.MemDb.HasStatVar(id) {
		return &pb.GetStatVarPathResponse{
			Path: []string{id, "dc/g/Private"},
		}, nil
	}

	path := []string{id}
	curr := id
	for {
		if parents, ok := cache.ParentSvg[curr]; ok {
			curr = parents[0]
			if curr == svgRoot {
				break
			}
			path = append(path, curr)
		} else {
			break
		}
	}
	return &pb.GetStatVarPathResponse{
		Path: path,
	}, nil
}
