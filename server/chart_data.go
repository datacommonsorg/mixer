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
	"strings"

	pb "github.com/datacommonsorg/mixer/proto"
)

// GetChartData implements API for Mixer.GetChartData.
func (s *Server) GetChartData(ctx context.Context,
	in *pb.GetChartDataRequest) (*pb.GetChartDataResponse, error) {
	keys := in.GetKeys()
	if len(keys) == 0 {
		return nil, fmt.Errorf("missing required arguments")
	}
	result := map[string]*pb.ObsTimeSeries{}
	rowList := buildChartDataKey(keys)

	// Read from branch cache first
	memData := s.memcache.ReadParallel(rowList, convertToObsSeries, true)
	for key, data := range memData {
		result[key] = data.(*pb.ObsTimeSeries)
	}
	// Read data from Bigtable if not all data is obtained from memcache.
	if len(memData) < len(keys) {
		dataMap, err := bigTableReadRowsParallel(
			ctx, s.btTable, rowList,
			convertToObsSeries,
			true,
		)
		if err != nil {
			return nil, err
		}
		for key, data := range dataMap {
			if _, ok := result[key]; !ok {
				result[key] = data.(*pb.ObsTimeSeries)
			}
		}
	}
	for key := range result {
		result[key].PlaceDcid = strings.Split(result[key].PlaceDcid, "^")[0]
	}

	for dcid := range result {
		result[dcid] = filterAndRank(result[dcid], "", "", "")
	}

	jsonRaw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return &pb.GetChartDataResponse{Payload: string(jsonRaw)}, nil
}
