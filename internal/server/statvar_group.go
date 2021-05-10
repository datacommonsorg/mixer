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

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
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

// GetStatVarGroup implements API for Mixer.GetStatVarGroupRequest.
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
