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

// API Implementation for /v1/bulk/point/...

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

// BulkPoint implements API for Mixer.ObservationsBulkPoint.
func BulkPoint(
	ctx context.Context,
	in *pb.BulkObservationsPointRequest,
	store *store.Store,
) (*pb.BulkObservationsPointResponse, error) {
	entities := in.GetEntities()
	variables := in.GetVariables()
	date := in.GetDate()
	allFacets := in.GetAllFacets()

	rowList, keyTokens := bigtable.BuildObsTimeSeriesKey(entities, variables)
	cacheData, err := stat.ReadStatsPb(ctx, store.BtGroup, rowList, keyTokens)
	if err != nil {
		return nil, err
	}

	result := &pb.BulkObservationsPointResponse{
		Facets: map[uint32]*pb.StatMetadata{},
	}
	tmpResult := map[string]*pb.VariableObservations{}
	for _, entity := range entities {
		entityData, ok := cacheData[entity]
		if !ok {
			continue
		}
		for _, variable := range variables {
			obsTimeSeries, ok := entityData[variable]
			if !ok || obsTimeSeries == nil {
				continue
			}
			if _, ok := tmpResult[variable]; !ok {
				tmpResult[variable] = &pb.VariableObservations{
					Variable: variable,
				}
			}
			entityObservations := &pb.EntityObservations{
				Entity: entity,
			}
			series := obsTimeSeries.SourceSeries
			sort.Sort(ranking.SeriesByRank(series))

			if !allFacets && len(series) > 0 {
				series = series[0:1]
			}

			for _, series := range series {
				metadata := stat.GetMetadata(series)
				facet := util.GetMetadataHash(metadata)
				// Date is given
				if date != "" {
					if value, ok := series.Val[date]; ok {
						ps := &pb.PointStat{
							Date:  date,
							Value: value,
							Facet: facet,
						}
						entityObservations.ObservationsByFacet = append(
							entityObservations.ObservationsByFacet, ps)
					}
				} else {
					// Date is not given, find the latest value
					latestDate := ""
					var ps *pb.PointStat
					for date, value := range series.Val {
						if date > latestDate {
							latestDate = date
							ps = &pb.PointStat{
								Date:  date,
								Value: value,
								Facet: facet,
							}
						}
					}
					entityObservations.ObservationsByFacet = append(
						entityObservations.ObservationsByFacet, ps)
				}
				result.Facets[facet] = metadata
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
