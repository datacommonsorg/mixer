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
	"github.com/datacommonsorg/mixer/internal/server/ranking"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
)

// BulkSeries implements API for Mixer.BulkObservationsSeries.
func BulkSeries(
	ctx context.Context,
	in *pb.BulkObservationsSeriesRequest,
	store *store.Store,
) (*pb.BulkObservationsSeriesResponse, error) {
	entities := in.GetEntities()
	variables := in.GetVariables()
	allFacets := in.GetAllFacets()
	customImportGroups := in.GetCustomImportGroups()
	// Add custom import groups to the context so bigtable reader can use it.
	ctx = context.WithValue(ctx, bigtable.CustomImportGroups, customImportGroups)

	result := &pb.BulkObservationsSeriesResponse{
		Facets: map[uint32]*pb.StatMetadata{},
	}
	btData, err := stat.ReadStatsPb(ctx, store.BtGroup, entities, variables)
	if err != nil {
		return result, err
	}

	tmpResult := map[string]*pb.VariableObservations{}
	for _, entity := range entities {
		for _, variable := range variables {
			series := btData[entity][variable].SourceSeries
			entityObservations := &pb.EntityObservations{
				Entity: entity,
			}
			if _, ok := tmpResult[variable]; !ok {
				tmpResult[variable] = &pb.VariableObservations{
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
					metadata := stat.GetMetadata(series)
					facet := util.GetMetadataHash(metadata)
					timeSeries := &pb.TimeSeries{
						Facet: facet,
					}
					for date, value := range series.Val {
						ps := &pb.PointStat{
							Date:  date,
							Value: value,
						}
						timeSeries.Series = append(timeSeries.Series, ps)
						sort.SliceStable(timeSeries.Series, func(i, j int) bool {
							return timeSeries.Series[i].Date < timeSeries.Series[j].Date
						})
					}
					entityObservations.SeriesByFacet = append(
						entityObservations.SeriesByFacet,
						timeSeries,
					)
					result.Facets[facet] = metadata
				}
			} else if store.MemDb.HasStatVar(variable) {
				// Read series from in-memory database
				series := store.MemDb.ReadSeries(variable, entity)
				if !allFacets && len(series) > 0 {
					series = series[0:1]
				}
				for _, series := range series {
					facet := util.GetMetadataHash(series.Metadata)
					timeSeries := &pb.TimeSeries{
						Facet: facet,
					}
					for date, value := range series.Val {
						ps := &pb.PointStat{
							Date:  date,
							Value: value,
						}
						timeSeries.Series = append(timeSeries.Series, ps)
						sort.SliceStable(timeSeries.Series, func(i, j int) bool {
							return timeSeries.Series[i].Date < timeSeries.Series[j].Date
						})
					}
					entityObservations.SeriesByFacet = append(
						entityObservations.SeriesByFacet,
						timeSeries,
					)
					result.Facets[facet] = series.Metadata
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
