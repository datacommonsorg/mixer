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
	"sort"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	// SvgRoot is the root stat var group of the hierarchy. It's a virtual entity
	// that links to the top level category stat var groups.
	SvgRoot         = "dc/g/Root"
	svgShadowRoot   = "dc/g/ShadowRoot"
	customSvgRoot   = "dc/g/Custom_Root"
	customSVGPrefix = "dc/g/Custom_"
)

// Note this function modifies validSVG inside.
func markValidSVG(
	svgResp *pb.StatVarGroups,
	svgID string,
	validSVG map[string]struct{},
) bool {
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

// Filter StatVarGroups based on given stat vars. This does not modify the input
// data but create a filtered copy of it.
func filterSVG(in *pb.StatVarGroups, statVars []string) *pb.StatVarGroups {
	result := &pb.StatVarGroups{StatVarGroups: map[string]*pb.StatVarGroupNode{}}
	// Build set for all the given stat vars as valid stat vars.
	validSV := map[string]struct{}{}
	for _, sv := range statVars {
		validSV[sv] = struct{}{}
	}

	// Step 1: iterate over stat var group, and only keep stat var children with
	// valid stat vars.
	for svgID, svgData := range in.StatVarGroups {
		filteredChildSV := []*pb.StatVarGroupNode_ChildSV{}
		for _, childSV := range svgData.ChildStatVars {
			if _, ok := validSV[childSV.Id]; ok {
				filteredChildSV = append(filteredChildSV, childSV)
			}
		}
		result.StatVarGroups[svgID] = &pb.StatVarGroupNode{
			ChildStatVars:      filteredChildSV,
			ChildStatVarGroups: svgData.ChildStatVarGroups,
		}
	}
	// Step 2: recursively check if a stat var group is valid. A stat var group
	// is valid if it has any descendent stat var group with non-empty stat vars

	// All the svg with valid stat vars.
	validSVG := map[string]struct{}{}
	for svgID := range result.StatVarGroups {
		markValidSVG(result, svgID, validSVG)
	}

	// Step3: another iteration to only keep valid svg
	for svgID, svgData := range result.StatVarGroups {
		filteredChildren := []*pb.StatVarGroupNode_ChildSVG{}
		for _, c := range svgData.ChildStatVarGroups {
			if _, ok := validSVG[c.Id]; ok {
				filteredChildren = append(filteredChildren, c)
			}
		}
		result.StatVarGroups[svgID].ChildStatVarGroups = filteredChildren
		d := result.StatVarGroups[svgID]
		if len(d.ChildStatVars) == 0 && len(d.ChildStatVarGroups) == 0 {
			delete(result.StatVarGroups, svgID)
		}
	}
	return result
}

// adjustDescendentStatVarCount returns all the unique stat var under curID.
// Recursively mutates DescendentStatVarCount for all nodes in id2Node.
func adjustDescendentSVCount(
	id2Node map[string]*pb.StatVarGroupNode, curID string,
) map[string]struct{} {
	curNode, ok := id2Node[curID]
	if !ok {
		return nil
	}
	descendentSV := map[string]struct{}{}
	for _, sv := range curNode.ChildStatVars {
		descendentSV[sv.Id] = struct{}{}
	}
	for _, childSVG := range curNode.GetChildStatVarGroups() {
		childDescendentSV := adjustDescendentSVCount(id2Node, childSVG.GetId())
		// Child SVG protos have its own DescendentStatVarCount that must be set.
		childSVG.DescendentStatVarCount = int32(len(childDescendentSV))
		for sv := range childDescendentSV {
			descendentSV[sv] = struct{}{}
		}
	}
	curNode.DescendentStatVarCount = int32(len(descendentSV))
	return descendentSV
}

// mergeSVGNodes merges node2's svg and svs into node1.
// Merge here refers to set operation.
// Warning: node1's DescendentStatVarCount is unchanged.
// Caller is responsible for calling fixCustomRootDescendentStatVarCount
// at the end of all merges.
func mergeSVGNodes(node1, node2 *pb.StatVarGroupNode) {
	node1SVGs := map[string]bool{}
	for _, childSVG := range node1.GetChildStatVarGroups() {
		node1SVGs[childSVG.GetId()] = true
	}
	for _, childSVG := range node2.GetChildStatVarGroups() {
		if _, ok := node1SVGs[childSVG.GetId()]; !ok {
			node1.ChildStatVarGroups = append(node1.ChildStatVarGroups, childSVG)
			node1SVGs[childSVG.GetId()] = true
		}
	}

	node1SVs := map[string]bool{}
	for _, childSV := range node1.GetChildStatVars() {
		node1SVs[childSV.GetId()] = true
	}
	for _, childSV := range node2.GetChildStatVars() {
		if _, ok := node1SVs[childSV.GetId()]; !ok {
			node1.ChildStatVars = append(node1.ChildStatVars, childSV)
			node1SVs[childSV.GetId()] = true
		}
	}
}

// GetStatVarGroup implements API for Mixer.GetStatVarGroup.
func GetStatVarGroup(
	ctx context.Context,
	in *pb.GetStatVarGroupRequest,
	store *store.Store,
	cache *resource.Cache,
	pinnedSvg string,
) (*pb.StatVarGroups, error) {
	defer util.TimeTrack(time.Now(), "GetStatVarGroup")
	entities := in.GetEntities()
	var statVars []string
	// Only read entity stat vars when the entity is provided.
	// User can provide any arbitrary dcid, which might not be associated with
	// stat vars. In this case, an empty response is returned.
	if len(entities) > 0 {
		svUnionResp, err := GetEntityStatVarsUnionV1(
			ctx,
			&pb.GetEntityStatVarsUnionRequest{Dcids: entities},
			store,
		)
		if err != nil {
			return nil, err
		}
		statVars = svUnionResp.StatVars
	}

	result := &pb.StatVarGroups{StatVarGroups: map[string]*pb.StatVarGroupNode{}}
	if cache == nil {
		// Read stat var group cache from the frequent import group table. It has
		// the latest and trustworthy stat var schemas and no need to merge with
		// other import groups.
		btDataList, err := bigtable.Read(
			ctx,
			store.BtGroup,
			bigtable.BtStatVarGroup,
			[][]string{{""}},
			func(jsonRaw []byte) (interface{}, error) {
				var svgResp pb.StatVarGroups
				if err := proto.Unmarshal(jsonRaw, &svgResp); err != nil {
					return nil, err
				}
				return &svgResp, nil
			},
		)
		if err != nil {
			return nil, err
		}
		// Loop through import group by order. The stat var group is preferred from
		// a higher ranked import group.
		var customRootNode *pb.StatVarGroupNode
		for _, btData := range btDataList {
			for _, row := range btData {
				svgData, ok := row.Data.(*pb.StatVarGroups)
				if ok && len(svgData.StatVarGroups) > 0 {
					for k, v := range svgData.StatVarGroups {
						if k == customSvgRoot && customRootNode == nil {
							customRootNode = v
						}
						if _, ok := result.StatVarGroups[k]; !ok {
							result.StatVarGroups[k] = v
						} else {
							// Merge all SVGs regardless of the import group rank as one SVG
							// can exist in multiple import group.
							mergeSVGNodes(result.StatVarGroups[k], v)
						}
					}
				}
			}
		}
		if customRootNode != nil {
			customRootExist := false
			// If custom schema is built together with base schema, then it is
			// already in the child stat var group of "dc/g/Root".
			for _, x := range result.StatVarGroups[SvgRoot].ChildStatVarGroups {
				if x.Id == customSvgRoot {
					customRootExist = true
					break
				}
			}
			// Populate dc/g/Custom_Root as children of dc/g/Root
			if !customRootExist {
				result.StatVarGroups[SvgRoot].ChildStatVarGroups = append(
					result.StatVarGroups[SvgRoot].ChildStatVarGroups,
					&pb.StatVarGroupNode_ChildSVG{
						Id:                customSvgRoot,
						SpecializedEntity: customRootNode.AbsoluteName,
					},
				)
			}
		}
	} else {
		result = &pb.StatVarGroups{StatVarGroups: cache.RawSvg}
	}
	// Merge in the private import svg if exists
	if store.MemDb != nil && store.MemDb.GetSvg() != nil {
		for sv, data := range store.MemDb.GetSvg() {
			result.StatVarGroups[sv] = data
		}
		result.StatVarGroups[SvgRoot].ChildStatVarGroups = append(
			result.StatVarGroups[SvgRoot].ChildStatVarGroups,
			&pb.StatVarGroupNode_ChildSVG{
				Id:                store.MemDb.GetManifest().RootSvg,
				SpecializedEntity: store.MemDb.GetManifest().ImportName,
				DisplayName:       store.MemDb.GetManifest().ImportName,
			},
		)
	}
	// If there is a re-grouping svg under the root, then do the regrouping
	if pinnedSvg != "" {
		newChildren := []*pb.StatVarGroupNode_ChildSVG{}
		var pinnedSvgNode *pb.StatVarGroupNode_ChildSVG
		for _, child := range result.StatVarGroups[SvgRoot].ChildStatVarGroups {
			if child.Id == pinnedSvg {
				child.SpecializedEntity = "Imported by " + child.SpecializedEntity
				pinnedSvgNode = child
			} else {
				newChildren = append(newChildren, child)
			}
		}
		if pinnedSvgNode != nil {
			totalCount := result.StatVarGroups[SvgRoot].DescendentStatVarCount
			result.StatVarGroups[SvgRoot].ChildStatVarGroups = []*pb.StatVarGroupNode_ChildSVG{
				{
					Id:                     svgShadowRoot,
					SpecializedEntity:      "Imported by Google",
					DescendentStatVarCount: totalCount - pinnedSvgNode.DescendentStatVarCount,
				},
				pinnedSvgNode,
			}
			result.StatVarGroups[svgShadowRoot] = &pb.StatVarGroupNode{
				ChildStatVarGroups:     newChildren,
				DescendentStatVarCount: totalCount - pinnedSvgNode.DescendentStatVarCount,
			}
		}
	}

	// Recount all descendent stat vars after merging
	adjustDescendentSVCount(result.StatVarGroups, SvgRoot)
	if len(entities) > 0 {
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
		allIDs = append(allIDs, result.ParentStatVarGroups...)
		// Check if stat data exists for given entities
		statVarCount, err := Count(ctx, store, allIDs, entities)
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
