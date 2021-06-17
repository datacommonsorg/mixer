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

package server

import (
	"context"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	svgRoot            = "dc/g/Root"
	autoGenSvgIDPrefix = "dc/g/"
	svgDelimiter       = "_"
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

func filterSVG(svgResp *pb.StatVarGroups, placeSVs []string) *pb.StatVarGroups {
	// Build set for all the SV.
	validSV := map[string]struct{}{}
	for _, sv := range placeSVs {
		validSV[sv] = struct{}{}
	}

	// Step 1: iterate over stat var group, and only keep stat var children with valid
	// stat vars for this place.
	for _, svgData := range svgResp.StatVarGroups {
		filteredChildren := []*pb.StatVarGroupNode_ChildSV{}
		for _, child := range svgData.ChildStatVars {
			if _, ok := validSV[child.Id]; ok {
				filteredChildren = append(filteredChildren, child)
			}
		}
		svgData.ChildStatVars = filteredChildren
	}

	// Step 2: recursively check if a stat var group is valid. A stat var group
	// is valid if it has any descendent stat var group with non-empty stat vars

	// All the svg with valid sv for this place
	validSVG := map[string]struct{}{}

	for svgID := range svgResp.StatVarGroups {
		markValidSVG(svgResp, svgID, validSVG)
	}

	// Step3: another iteration to only keep valid svg
	for svgID, svgData := range svgResp.StatVarGroups {
		filteredChildren := []*pb.StatVarGroupNode_ChildSVG{}
		for _, c := range svgData.ChildStatVarGroups {
			if _, ok := validSVG[c.Id]; ok {
				filteredChildren = append(filteredChildren, c)
			}
		}
		svgData.ChildStatVarGroups = filteredChildren
		if len(svgData.ChildStatVars) == 0 && len(svgData.ChildStatVarGroups) == 0 {
			delete(svgResp.StatVarGroups, svgID)
		}
	}
	return svgResp
}

// GetStatVarGroup implements API for Mixer.GetStatVarGroup.
func (s *Server) GetStatVarGroup(
	ctx context.Context, in *pb.GetStatVarGroupRequest) (
	*pb.StatVarGroups, error) {
	places := in.GetPlaces()

	var statVars []string
	svgResp := &pb.StatVarGroups{}

	// Only read place stat vars when the place is provided.
	// User can provide any arbitrary dcid, which might not be associated with
	// stat vars. In this case, an empty response is returned.
	if len(places) > 0 {
		svUnionResp, err := s.GetPlaceStatVarsUnion(
			ctx, &pb.GetPlaceStatVarsUnionRequest{Dcids: places})
		if err != nil {
			return nil, err
		}
		statVars = svUnionResp.StatVars.StatVars
	}

	// Read stat var group cache data
	row, err := s.store.BaseBt().ReadRow(ctx, util.BtStatVarGroup)
	if err != nil {
		return nil, err
	}
	if len(row[util.BtFamily]) == 0 {
		return nil, status.Errorf(codes.NotFound, "Stat Var Group not found in cache")
	}
	raw := row[util.BtFamily][0].Value
	jsonRaw, err := util.UnzipAndDecode(string(raw))
	if err != nil {
		return nil, err
	}
	err = protojson.Unmarshal(jsonRaw, svgResp)
	if err != nil {
		return nil, err
	}

	if len(places) > 0 {
		svgResp = filterSVG(svgResp, statVars)
	}
	return svgResp, nil
}

// GetStatVarGroupNode implements API for Mixer.GetStatVarGroupNode.
func (s *Server) GetStatVarGroupNode(
	ctx context.Context, in *pb.GetStatVarGroupNodeRequest) (
	*pb.StatVarGroupNode, error) {
	places := in.GetPlaces()
	svg := in.GetStatVarGroup()

	if svg == "" {
		return nil, status.Errorf(
			codes.InvalidArgument, "Missing required argument: stat_var_group")
	}

	triples, err := readTriples(ctx, s.store, buildTriplesKey([]string{svg}))
	if err != nil {
		return nil, err
	}

	if _, ok := triples[svg]; !ok {
		return nil, status.Errorf(
			codes.Internal, "No triples for stat var group: %s", svg)
	}

	result := &pb.StatVarGroupNode{}
	if in.GetReadFromTriples() {
		// Go through triples and populate result fields.
		for _, t := range triples[svg].Triples {
			if t.SubjectID == svg {
				// SVG is subject
				if t.Predicate == "specializationOf" {
					// Parent SVG
					result.ParentStatVarGroups = append(result.ParentStatVarGroups, t.ObjectID)
				} else if t.Predicate == "name" {
					result.AbsoluteName = t.ObjectValue
				}
			} else {
				// SVG is object
				if t.Predicate == "specializationOf" {
					// Children SVG
					result.ChildStatVarGroups = append(result.ChildStatVarGroups,
						&pb.StatVarGroupNode_ChildSVG{
							Id:                t.SubjectID,
							DisplayName:       t.SubjectName,
							SpecializedEntity: computeSpecializedEntity(svg, t.SubjectID),
						})
				} else if t.Predicate == "memberOf" {
					// Children SV
					result.ChildStatVars = append(result.ChildStatVars,
						&pb.StatVarGroupNode_ChildSV{
							Id:          t.SubjectID,
							DisplayName: t.SubjectName,
						})
				}
			}
		}
	} else {
		result = s.cache.SvgInfo[svg]
		for _, item := range result.ChildStatVarGroups {
			item.DisplayName = s.cache.SvgInfo[item.Id].AbsoluteName
		}
		result.ParentStatVarGroups = s.cache.ParentSvg[svg]
	}

	// Get the stat var and stat var group IDs to check if they are valid for
	// given places.
	allIDs := []string{}
	for _, item := range result.ChildStatVarGroups {
		allIDs = append(allIDs, item.Id)
	}
	for _, item := range result.ChildStatVars {
		allIDs = append(allIDs, item.Id)
	}
	allIDs = append(allIDs, result.ParentStatVarGroups...)

	// Check if stat data exists for given places
	statExistence, err := checkStatExistence(ctx, s.store, allIDs, places)
	if err != nil {
		return nil, err
	}

	// Filter result based on places
	// TODO(shifucun): Find a generic way to do the filtering here.
	// Filter parent stat var groups
	if len(places) > 0 {
		filteredParentStatVarGroups := []string{}
		for _, item := range result.ParentStatVarGroups {
			if c, ok := statExistence[item]; ok && c == len(places) {
				filteredParentStatVarGroups = append(filteredParentStatVarGroups, item)
			}
		}
		result.ParentStatVarGroups = filteredParentStatVarGroups
		// Filter child stat var groups
		filteredChildStatVarGroups := []*pb.StatVarGroupNode_ChildSVG{}
		for _, item := range result.ChildStatVarGroups {
			if c, ok := statExistence[item.Id]; ok && c == len(places) {
				filteredChildStatVarGroups = append(filteredChildStatVarGroups, item)
			}
		}
		result.ChildStatVarGroups = filteredChildStatVarGroups
		// Filter child stat vars
		filteredChildStatVars := []*pb.StatVarGroupNode_ChildSV{}
		for _, item := range result.ChildStatVars {
			if c, ok := statExistence[item.Id]; ok && c == len(places) {
				filteredChildStatVars = append(filteredChildStatVars, item)
			}
		}
		result.ChildStatVars = filteredChildStatVars
	}
	return result, nil
}

// GetStatVarPath implements API for Mixer.GetStatVarPath.
func (s *Server) GetStatVarPath(
	ctx context.Context, in *pb.GetStatVarPathRequest) (
	*pb.GetStatVarPathResponse, error) {
	id := in.GetId()
	if id == "" {
		return nil, status.Errorf(
			codes.InvalidArgument, "Missing required argument: id")
	}
	path := []string{id}
	curr := id
	for {
		if parents, ok := s.cache.ParentSvg[curr]; ok {
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

func isBasicPopulationType(t string) bool {
	// Household and HousingUnit are included here because they have corresponding
	// verticals.
	return t == "Person" || t == "Household" || t == "HousingUnit" ||
		t == "Thing"
}

func computeSpecializedEntity(parentSvg string, childSvg string) string {
	// We compute this only for auto-generated IDs.
	if !strings.HasPrefix(parentSvg, autoGenSvgIDPrefix) ||
		!strings.HasPrefix(childSvg, autoGenSvgIDPrefix) {
		return ""
	}
	parentPieces := strings.Split(
		strings.TrimPrefix(parentSvg, autoGenSvgIDPrefix), svgDelimiter)
	parentSet := map[string]struct{}{}
	for _, p := range parentPieces {
		parentSet[p] = struct{}{}
	}

	childPieces := strings.Split(
		strings.TrimPrefix(childSvg, autoGenSvgIDPrefix), svgDelimiter)
	result := []string{}
	for _, c := range childPieces {
		if isBasicPopulationType(c) {
			continue
		}
		if _, ok := parentSet[c]; ok {
			continue
		}
		result = append(result, c)
	}
	if len(result) == 0 {
		// Edge case: certain SVGs (e.g., Person_Employment) match the parent
		// (Employment) after stripping Person from the name.
		result = parentPieces
	}
	return strings.Join(result, ", ")
}

// Check if places have data for stat vars and stat var groups
// Returns a map from stat var to the number of places that has data.
func checkStatExistence(
	ctx context.Context,
	store *store.Store,
	svOrSvgs []string,
	places []string) (map[string]int, error) {
	rowList, keyTokens := buildStatExistenceKey(places, svOrSvgs)
	keyToTokenFn := tokenFn(keyTokens)
	baseDataMap, _, err := bigTableReadRowsParallel(
		ctx, store, rowList, func(string, []byte) (interface{}, error) {
			// If exist, BT read returns an empty struct. Here just return nil to
			// indicate the existence of the key.
			return nil, nil
		}, keyToTokenFn,
	)
	if err != nil {
		return nil, err
	}
	result := map[string]int{}
	for _, rowKey := range rowList {
		placeSv := keyTokens[rowKey]
		token, _ := keyToTokenFn(rowKey)
		if _, ok := baseDataMap[token]; ok {
			result[placeSv.statVar]++
		}
	}
	return result, nil
}
