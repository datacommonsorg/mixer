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
	"encoding/json"
	"log"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

func filterSVG(svgResp *pb.StatVarGroups, placeSVs []string) *pb.StatVarGroups {
	// Build set for all the SV.
	svSet := map[string]struct{}{}
	for _, sv := range placeSVs {
		svSet[sv] = struct{}{}
	}

	// All the svg with valid sv for this place
	svgSet := map[string]struct{}{}

	// Iterate over stat var group, and only keep children with valid stat vars
	// for this place.
	for svgID, svgData := range svgResp.StatVarGroups {
		filteredChildren := []string{}
		for _, child := range svgData.ChildStatVars {
			if _, ok := svSet[child]; ok {
				filteredChildren = append(filteredChildren, child)
			}
		}
		svgData.ChildStatVars = filteredChildren
		if len(filteredChildren) > 0 {
			svgSet[svgID] = struct{}{}
		}
	}

	// Another iteration to remove SVG without svg children nor sv children
	for svgID, svgData := range svgResp.StatVarGroups {
		filteredChildren := []*pb.StatVarGroupNode_Child{}
		for _, c := range svgData.ChildStatVarGroups {
			if _, ok := svgSet[c.Id]; ok {
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
	place := in.GetPlace()
	log.Println(place)
	svobsMode := s.metadata.SvObsMode

	if !svobsMode {
		return nil, status.Errorf(codes.NotFound,
			"GetStatVarGroup is not implemented for PopObs mode")
	}

	// Read stat var group cache data
	row, err := s.store.BaseBt().ReadRow(ctx, util.BtStatVarGroup)
	if err != nil {
		return nil, err
	}
	raw := row[util.BtFamily][0].Value
	jsonRaw, err := util.UnzipAndDecode(string(raw))
	if err != nil {
		return nil, err
	}
	svgResp := &pb.StatVarGroups{}
	err = protojson.Unmarshal(jsonRaw, svgResp)
	if err != nil {
		return nil, err
	}

	// Read place statvars
	if len(svgResp.StatVarGroups) > 0 {
		row, err := s.store.BaseBt().ReadRow(ctx, util.BtPlaceStatsVarPrefix+place)
		if err != nil {
			return nil, err
		}
		raw := row[util.BtFamily][0].Value
		jsonRaw, err := util.UnzipAndDecode(string(raw))
		if err != nil {
			return nil, err
		}
		var sv PlaceStatsVar
		err = json.Unmarshal(jsonRaw, &sv)
		if err != nil {
			return nil, err
		}
		svgResp = filterSVG(svgResp, sv.StatVarIds)
	}
	return svgResp, nil
}
