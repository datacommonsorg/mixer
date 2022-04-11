// Copyright 2022 Google LLC
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

package observations

import (
	"context"
	"sort"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/ranking"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Series implements API for Mixer.ObservationsSeries.
func Series(
	ctx context.Context,
	in *pb.ObservationsSeriesRequest,
	store *store.Store,
) (*pb.ObservationsSeriesResponse, error) {
	entity := in.GetEntity()
	variable := in.GetVariable()

	if entity == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: entity")
	}
	if variable == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: variable")
	}
	resp := &pb.ObservationsSeriesResponse{}
	rowList, keyTokens := bigtable.BuildObsTimeSeriesKey([]string{entity}, []string{variable})
	btData, err := stat.ReadStatsPb(ctx, store.BtGroup, rowList, keyTokens)
	if err != nil {
		return resp, err
	}
	entityData, ok := btData[entity]
	if !ok {
		return resp, err
	}
	variableData, ok := entityData[variable]
	if !ok {
		return resp, err
	}
	series := variableData.SourceSeries
	if len(series) == 0 {
		return resp, err
	}
	sort.Sort(ranking.SeriesByRank(series))
	resp.Facet = stat.GetMetadata(series[0])
	dates := []string{}
	for date := range series[0].Val {
		dates = append(dates, date)
	}
	sort.Strings(dates)
	for _, date := range dates {
		resp.Observations = append(resp.Observations, &pb.PointStat{
			Date:  date,
			Value: series[0].Val[date],
		})
	}
	return resp, nil
}
