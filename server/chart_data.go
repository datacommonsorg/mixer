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
)

var transform = func(key string, jsonRaw []byte) (interface{}, error) {
	var chartStore ChartStore
	err := json.Unmarshal(jsonRaw, &chartStore)
	if err != nil {
		return nil, err
	}
	return &chartStore, nil
}

// GetChartData implements API for Mixer.GetChartData.
func (s *Server) GetChartData(ctx context.Context,
	in *pb.GetChartDataRequest) (*pb.GetChartDataResponse, error) {
	keys := in.GetKeys()
	if len(keys) == 0 {
		return nil, fmt.Errorf("missing required arguments")
	}
	result := map[string]*ChartStore{}
	rowList := buildChartDataKey(keys)

	// Read from branch cache first
	memData := s.memcache.ReadParallel(rowList, transform, true)
	for dcid, data := range memData {
		result[dcid] = data.(*ChartStore)
	}
	// Read data from Bigtable if no data exists from Memcache
	if len(memData) == 0 {
		dataMap, err := bigTableReadRowsParallel(
			ctx, s.btTable, rowList,
			transform,
			true,
		)
		if err != nil {
			return nil, err
		}
		for key, data := range dataMap {
			result[key] = data.(*ChartStore)
		}
	}

	jsonRaw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return &pb.GetChartDataResponse{Payload: string(jsonRaw)}, nil
}
