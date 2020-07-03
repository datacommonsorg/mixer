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
	"fmt"

	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/util"
)

// GetPropertyLabelsPost implements API for Mixer.GetPropertyLabelsPost.
func (s *Server) GetPropertyLabelsPost(ctx context.Context,
	in *pb.GetPropertyLabelsRequest) (*pb.GetPropertyLabelsResponse, error) {
	return s.GetPropertyLabels(ctx, in)
}

// GetPropertyLabels implements API for Mixer.GetPropertyLabels.
func (s *Server) GetPropertyLabels(ctx context.Context,
	in *pb.GetPropertyLabelsRequest) (*pb.GetPropertyLabelsResponse, error) {
	dcids := in.GetDcids()
	if len(dcids) == 0 {
		return nil, fmt.Errorf("missing required arguments")
	}
	if !util.CheckValidDCIDs(dcids) {
		return nil, fmt.Errorf("invalid DCIDs")
	}

	rowList := buildPropertyLabelKey(dcids)

	dataMap, err := bigTableReadRowsParallel(
		ctx, s.btTable, rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			var propLabels PropLabelCache
			err := json.Unmarshal(jsonRaw, &propLabels)
			if err != nil {
				return nil, err
			}
			return &propLabels, nil
		})
	if err != nil {
		return nil, err
	}
	result := map[string]*PropLabelCache{}
	for dcid, data := range dataMap {
		result[dcid] = data.(*PropLabelCache)
		// Fill in InLabels / OutLabels with an empty list if not present.
		if result[dcid].InLabels == nil {
			result[dcid].InLabels = []string{}
		}
		if result[dcid].OutLabels == nil {
			result[dcid].OutLabels = []string{}
		}
	}
	// Iterate through all dcids to make sure they are present in result.
	for _, dcid := range dcids {
		if _, exists := result[dcid]; !exists {
			result[dcid] = &PropLabelCache{
				InLabels:  []string{},
				OutLabels: []string{},
			}
		}
	}
	jsonRaw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return &pb.GetPropertyLabelsResponse{Payload: string(jsonRaw)}, nil
}
