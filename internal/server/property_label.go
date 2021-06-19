// Copyright 2020 Google LLC
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

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetPropertyLabels implements API for Mixer.GetPropertyLabels.
func (s *Server) GetPropertyLabels(ctx context.Context,
	in *pb.GetPropertyLabelsRequest) (*pb.GetPropertyLabelsResponse, error) {
	dcids := in.GetDcids()
	if len(dcids) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Missing required arguments: dcid")
	}
	if !util.CheckValidDCIDs(dcids) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid DCIDs")
	}

	rowList := buildPropertyLabelKey(dcids)

	baseDataMap, branchDataMap, err := bigTableReadRowsParallel(
		ctx,
		s.store,
		rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			var propLabels PropLabelCache
			err := json.Unmarshal(jsonRaw, &propLabels)
			if err != nil {
				return nil, err
			}
			return &propLabels, nil
		},
		nil,
		true,
	)
	if err != nil {
		return nil, err
	}
	result := map[string]*PropLabelCache{}
	for _, dcid := range dcids {
		result[dcid] = &PropLabelCache{InLabels: []string{}, OutLabels: []string{}}
		// Merge cache value from base and branch cache
		for _, m := range []map[string]interface{}{baseDataMap, branchDataMap} {
			if data, ok := m[dcid]; ok {
				if data.(*PropLabelCache).InLabels != nil {
					result[dcid].InLabels = util.MergeDedupe(
						result[dcid].InLabels, data.(*PropLabelCache).InLabels)
				}
				if data.(*PropLabelCache).OutLabels != nil {
					result[dcid].OutLabels = util.MergeDedupe(
						result[dcid].OutLabels, data.(*PropLabelCache).OutLabels)
				}
			}
		}
	}
	jsonRaw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return &pb.GetPropertyLabelsResponse{Payload: string(jsonRaw)}, nil
}
