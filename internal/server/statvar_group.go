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
	"google.golang.org/protobuf/proto"
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
		svUnionResp, err := s.GetPlaceStatVarsUnionV1(
			ctx, &pb.GetPlaceStatVarsUnionRequest{Dcids: places})
		if err != nil {
			return nil, err
		}
		statVars = svUnionResp.StatVars
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

	result := &pb.StatVarGroupNode{}

	if in.GetReadFromTriples() {
		triples, err := readTriples(ctx, s.store, buildTriplesKey([]string{svg}))
		if err != nil {
			return nil, err
		}

		if _, ok := triples[svg]; !ok {
			return nil, status.Errorf(
				codes.Internal, "No triples for stat var group: %s", svg)
		}
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
		if r, ok := s.cache.SvgInfo[svg]; ok {
			// Clone into result, otherwise the server cache is modified.
			result = proto.Clone(r).(*pb.StatVarGroupNode)
		}
		for _, item := range result.ChildStatVarGroups {
			item.DisplayName = s.cache.SvgInfo[item.Id].AbsoluteName
			item.NumDescendentStatVars = s.cache.SvgInfo[item.Id].NumDescendentStatVars
		}
		for _, item := range result.ChildStatVars {
			item.HasData = true
		}
		result.ParentStatVarGroups = s.cache.ParentSvg[svg]
	}

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
		statVarCount, err := countStatVar(ctx, s.store, allIDs, places)
		if err != nil {
			return nil, err
		}
		// Count for current node.
		result.NumDescendentStatVars = 0
		if existence, ok := statVarCount[svg]; ok && len(existence) > 0 {
			for _, count := range existence {
				// Use the largest count among all places.
				if count > result.NumDescendentStatVars {
					result.NumDescendentStatVars = count
				}
			}
		}
		// Filter child stat var groups
		for _, item := range result.ChildStatVarGroups {
			item.NumDescendentStatVars = 0
			if existence, ok := statVarCount[item.Id]; ok && len(existence) > 0 {
				for _, count := range existence {
					// Use the largest count among all places
					if count > item.NumDescendentStatVars {
						item.NumDescendentStatVars = count
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
// Returns a two level map from stat var dcid to place dcid to the number of
// stat vars with data. For a given stat var, if a place has no data, it will
// not show up in the second level map.
func countStatVar(
	ctx context.Context,
	store *store.Store,
	svOrSvgs []string,
	places []string) (map[string]map[string]int32, error) {
	rowList, keyTokens := buildStatExistenceKey(places, svOrSvgs)
	keyToTokenFn := tokenFn(keyTokens)
	baseDataMap, _, err := bigTableReadRowsParallel(
		ctx,
		store,
		rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			var statVarExistence pb.PlaceStatVarExistence
			err := protojson.Unmarshal(jsonRaw, &statVarExistence)
			if err != nil {
				return nil, err
			}
			return &statVarExistence, nil
		},
		keyToTokenFn,
		false, /* readBranch */
	)
	if err != nil {
		return nil, err
	}
	// Initialize result
	result := map[string]map[string]int32{}
	for _, id := range svOrSvgs {
		result[id] = map[string]int32{}
	}
	// Populate the count
	for _, rowKey := range rowList {
		placeSv := keyTokens[rowKey]
		token, _ := keyToTokenFn(rowKey)
		if data, ok := baseDataMap[token]; ok {
			c := data.(*pb.PlaceStatVarExistence)
			result[placeSv.statVar][placeSv.place] = c.NumDescendentStatVars
		}
	}
	return result, nil
}

// GetStatVarSummary implements API for Mixer.GetStatVarSummary.
func (s *Server) GetStatVarSummary(
	ctx context.Context, in *pb.GetStatVarSummaryRequest) (
	*pb.GetStatVarSummaryResponse, error) {
	sv := in.GetStatVars()
	rowList := buildStatVarSummaryKey(sv)
	baseDataMap, _, err := bigTableReadRowsParallel(
		ctx,
		s.store,
		rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			var statVarSummary pb.StatVarSummary
			err := protojson.Unmarshal(jsonRaw, &statVarSummary)
			if err != nil {
				return nil, err
			}
			return &statVarSummary, nil
		},
		nil,
		false, /* readBranch */
	)
	if err != nil {
		return nil, err
	}
	result := &pb.GetStatVarSummaryResponse{
		StatVarSummary: map[string]*pb.StatVarSummary{},
	}
	for dcid, data := range baseDataMap {
		result.StatVarSummary[dcid] = data.(*pb.StatVarSummary)
	}
	return result, nil
}
