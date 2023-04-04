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
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/ranking"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Series implements API for Mixer.ObservationsSeries.
func Series(
	ctx context.Context,
	in *pbv1.ObservationsSeriesRequest,
	store *store.Store,
) (*pbv1.ObservationsSeriesResponse, error) {
	entityVariable := in.GetEntityVariable()
	parts := strings.Split(entityVariable, "/")
	if len(parts) < 2 {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid request URI")
	}
	variable := parts[len(parts)-1]
	entity := strings.Join(parts[0:len(parts)-1], "/")

	if entity == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: entity")
	}
	if variable == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: variable")
	}
	resp := &pbv1.ObservationsSeriesResponse{}
	btData, err := stat.ReadStatsPb(
		ctx, store.BtGroup, []string{entity}, []string{variable})
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
	resp.Facet = util.GetFacet(series[0])
	dates := []string{}
	for date := range series[0].Val {
		dates = append(dates, date)
	}
	sort.Strings(dates)
	for _, date := range dates {
		resp.Observations = append(resp.Observations, &pb.PointStat{
			Date:  date,
			Value: proto.Float64(series[0].Val[date]),
		})
	}
	return resp, nil
}
