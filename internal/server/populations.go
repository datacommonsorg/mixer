// Copyright 2019 Google LLC
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
	"fmt"
	"sort"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

var migrationMSG = `
Missing argument: statVar

==== DEPRECATION NOTICE ====

The old /bulk/place-obs API has been deprecated in favor of a new data model
based on Statistical Variables: https://docs.datacommons.org/statistical_variables.html.

Please migrate your API to use the new model. Example request:

curl --request POST \
  --url https://api.datacommons.org/bulk/place-obs \
  --header 'content-type: application/json' \
  --data '{
		"placeType": "State",
		"statVar": "Count_Person",
		"date": "2015",
	}'
`

// GetPlaceObs implements API for Mixer.GetPlaceObs.
func (s *Server) GetPlaceObs(ctx context.Context, in *pb.GetPlaceObsRequest) (
	*pb.SVOCollection, error) {
	if s.store.BaseBt() == nil || s.store.BranchBt() == nil {
		return nil, status.Errorf(
			codes.NotFound, "Bigtable instance is not specified")
	}
	if in.GetPlaceType() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Missing argument: placeType")
	}
	if in.GetDate() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Missing argument: date")
	}
	if in.GetStatVar() == "" {
		return nil, status.Errorf(codes.InvalidArgument, migrationMSG)
	}

	key := fmt.Sprintf("%s%s^%s^%s", util.BtPlaceObsPrefix, in.GetPlaceType(),
		in.GetStatVar(), in.GetDate())

	// TODO(boxu): abstract out the common logic for handling cache merging.
	baseData := &pb.SVOCollection{}
	branchData := &pb.SVOCollection{}
	var baseRaw, branchRaw []byte
	var hasBaseData, hasBranchData bool

	btRow, err := s.store.BranchBt().ReadRow(ctx, key)
	if err != nil {
		return nil, err
	}
	hasBranchData = len(btRow[util.BtFamily]) > 0
	if hasBranchData {
		branchRaw = btRow[util.BtFamily][0].Value
		if tmp, err := util.UnzipAndDecode(string(branchRaw)); err == nil {
			err := protojson.Unmarshal(tmp, branchData)
			if err != nil {
				return nil, err
			}
		}
	}

	btRow, err = s.store.BaseBt().ReadRow(ctx, key)
	if err != nil {
		return nil, err
	}
	hasBaseData = len(btRow[util.BtFamily]) > 0
	if hasBaseData {
		baseRaw = btRow[util.BtFamily][0].Value
		if tmp, err := util.UnzipAndDecode(string(baseRaw)); err == nil {
			err := protojson.Unmarshal(tmp, baseData)
			if err != nil {
				return nil, err
			}
		}
	}
	dataMap := map[string]*pb.SVOPlace{}
	for _, data := range baseData.Places {
		dataMap[data.Dcid] = data
	}
	if hasBranchData {
		for _, data := range branchData.Places {
			dataMap[data.Dcid] = data
		}
	}

	places := []string{}
	for place := range dataMap {
		places = append(places, place)
	}
	sort.Strings(places)

	res := &pb.SVOCollection{}
	for _, place := range places {
		res.Places = append(res.Places, dataMap[place])
	}
	return res, err
}
