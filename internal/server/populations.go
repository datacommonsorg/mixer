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
	"log"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// GetPlaceObs implements API for Mixer.GetPlaceObs.
func (s *Server) GetPlaceObs(ctx context.Context, in *pb.GetPlaceObsRequest) (
	*pb.GetPlaceObsResponse, error) {
	if s.store.BaseBt() == nil || s.store.BranchBt() == nil {
		return nil, status.Errorf(
			codes.NotFound, "Bigtable instance is not specified")
	}
	if in.GetPlaceType() == "" || in.GetStatVar() == "" ||
		in.GetObservationDate() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "missing required arguments")
	}

	key := fmt.Sprintf("%s%s^%s^%s", util.BtPlaceObsPrefix, in.GetPlaceType(),
		in.GetStatVar(), in.GetObservationDate())
	out := pb.GetPlaceObsResponse{}

	// TODO(boxu): abstract out the common logic for handling cache merging.
	baseData := &pb.PopObsCollection{}
	branchData := &pb.PopObsCollection{}
	var baseRaw, branchRaw []byte
	var hasBaseData, hasBranchData bool
	out.Payload, _ = util.ZipAndEncode([]byte("{}"))

	btRow, err := s.store.BranchBt().ReadRow(ctx, key)
	if err != nil {
		log.Print(err)
	}
	hasBranchData = len(btRow[util.BtFamily]) > 0
	if hasBranchData {
		branchRaw = btRow[util.BtFamily][0].Value
	}

	btRow, err = s.store.BaseBt().ReadRow(ctx, key)
	if err != nil {
		log.Print(err)
	}
	hasBaseData = len(btRow[util.BtFamily]) > 0
	if hasBaseData {
		baseRaw = btRow[util.BtFamily][0].Value
	}

	if !hasBaseData && !hasBranchData {
		return &out, nil
	} else if !hasBaseData {
		out.Payload = string(branchRaw)
		return &out, nil
	} else if !hasBranchData {
		out.Payload = string(baseRaw)
		return &out, nil
	} else {
		if tmp, err := util.UnzipAndDecode(string(baseRaw)); err == nil {
			err := protojson.Unmarshal(tmp, baseData)
			if err != nil {
				return nil, err
			}
		}
		if tmp, err := util.UnzipAndDecode(string(branchRaw)); err == nil {
			err := protojson.Unmarshal(tmp, branchData)
			if err != nil {
				return nil, err
			}
		}
		dataMap := map[string]*pb.PopObsPlace{}
		for _, data := range baseData.Places {
			dataMap[data.Place] = data
		}
		for _, data := range branchData.Places {
			dataMap[data.Place] = data
		}
		res := &pb.PopObsCollection{}
		for _, v := range dataMap {
			res.Places = append(res.Places, v)
		}
		resBytes, err := protojson.Marshal(res)
		if err != nil {
			return &out, err
		}
		out.Payload, err = util.ZipAndEncode(resBytes)
		return &out, err
	}
}
