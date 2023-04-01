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

// API Implementation for /v1/bulk/observations/series/...

package observations

import (
	"context"
	"sort"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/ranking"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/proto"
)

// BulkSeries implements API for Mixer.BulkObservationsSeries.
func BulkSeries(
	ctx context.Context,
	in *pbv1.BulkObservationsSeriesRequest,
	store *store.Store,
) (*pbv1.BulkObservationsSeriesResponse, error) {
	entities := in.GetEntities()
	variables := in.GetVariables()
	allFacets := in.GetAllFacets()
	result := &pbv1.BulkObservationsSeriesResponse{
		Facets: map[string]*pb.StatMetadata{},
	}
	btData, err := stat.ReadStatsPb(ctx, store.BtGroup, entities, variables)
	if err != nil {
		return result, err
	}

	tmpResult := map[string]*pbv1.VariableObservations{}
	for _, entity := range entities {
		for _, variable := range variables {
			series := btData[entity][variable].SourceSeries
			if store.MemDb.HasStatVar(variable) {
				// Read series from in-memory database
				series = append(store.MemDb.ReadSeries(variable, entity), series...)
			}
			entityObservations := &pbv1.EntityObservations{
				Entity: entity,
			}
			if _, ok := tmpResult[variable]; !ok {
				tmpResult[variable] = &pbv1.VariableObservations{
					Variable: variable,
				}
			}
			if len(series) > 0 {
				// Read series from BT cache
				sort.Sort(ranking.SeriesByRank(series))
				if !allFacets && len(series) > 0 {
					series = series[0:1]
				}
				for _, series := range series {
					metadata := util.GetMetadata(series)
					facet := util.GetMetadataHash(metadata)
					timeSeries := &pbv1.TimeSeries{
						Facet: facet,
					}
					for date, value := range series.Val {
						ps := &pb.PointStat{
							Date:  date,
							Value: proto.Float64(value),
						}
						timeSeries.Series = append(timeSeries.Series, ps)
					}
					sort.SliceStable(timeSeries.Series, func(i, j int) bool {
						return timeSeries.Series[i].Date < timeSeries.Series[j].Date
					})
					entityObservations.SeriesByFacet = append(
						entityObservations.SeriesByFacet,
						timeSeries,
					)
					result.Facets[facet] = metadata
				}
			}
			tmpResult[variable].ObservationsByEntity = append(
				tmpResult[variable].ObservationsByEntity,
				entityObservations,
			)
		}
	}
	for _, variable := range variables {
		result.ObservationsByVariable = append(
			result.ObservationsByVariable, tmpResult[variable])
	}
	return result, nil
}
