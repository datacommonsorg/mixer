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

// API Implementation for /v1/bulk/observations/point

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

// BulkPoint implements API for Mixer.BulkObservationsPoint.
func BulkPoint(
	ctx context.Context,
	in *pbv1.BulkObservationsPointRequest,
	store *store.Store,
) (*pbv1.BulkObservationsPointResponse, error) {
	entities := in.GetEntities()
	variables := in.GetVariables()
	date := in.GetDate()
	allFacets := in.GetAllFacets()

	cacheData, err := stat.ReadStatsPb(ctx, store.BtGroup, entities, variables)
	if err != nil {
		return nil, err
	}

	result := &pbv1.BulkObservationsPointResponse{
		Facets: map[string]*pb.StatMetadata{},
	}
	tmpResult := map[string]*pbv1.VariableObservations{}
	for _, entity := range entities {
		for _, variable := range variables {
			series := cacheData[entity][variable].SourceSeries
			entityObservations := &pbv1.EntityObservations{
				Entity: entity,
			}
			if _, ok := tmpResult[variable]; !ok {
				tmpResult[variable] = &pbv1.VariableObservations{
					Variable: variable,
				}
			}
			if len(series) > 0 {
				sort.Sort(ranking.SeriesByRank(series))
				// When date is not given, tract the latest date from each series
				latestDateAcrossSeries := ""
				for idx, series := range series {
					metadata := stat.GetMetadata(series)
					facet := util.GetMetadataHash(metadata)
					// Date is given
					if date != "" {
						if value, ok := series.Val[date]; ok {
							ps := &pb.PointStat{
								Date:  date,
								Value: proto.Float64(value),
								Facet: facet,
							}
							entityObservations.PointsByFacet = append(
								entityObservations.PointsByFacet, ps)
						}
						result.Facets[facet] = metadata
						if !allFacets {
							break
						}
					} else {
						// This is to query from one facet and there is already data from
						// higher ranked facet. If the current facet is from an inferior
						// facet (like wikidata) then don't use it.
						// Such inferior facet is only used when there is no better facet
						// is prsent.
						if !allFacets && idx > 0 && stat.IsInferiorFacetPb(series) {
							break
						}
						var ps *pb.PointStat
						latestDate := ""
						for date, value := range series.Val {
							if date > latestDate {
								latestDate = date
								ps = &pb.PointStat{
									Date:  date,
									Value: proto.Float64(value),
									Facet: facet,
								}
							}
						}
						if idx == 0 || allFacets {
							entityObservations.PointsByFacet = append(
								entityObservations.PointsByFacet, ps)
						} else if latestDate > latestDateAcrossSeries {
							latestDateAcrossSeries = latestDate
							entityObservations.PointsByFacet[0] = ps
						}
					}
					result.Facets[facet] = metadata
				}
			} else if store.MemDb.HasStatVar(variable) {
				pointValue, facet := store.MemDb.ReadPointValue(variable, entity, date)
				if pointValue != nil {
					facetID := util.GetMetadataHash(facet)
					pointValue.Facet = facetID
					result.Facets[facetID] = facet
					entityObservations.PointsByFacet = append(
						entityObservations.PointsByFacet, pointValue)
				}
			}
			tmpResult[variable].ObservationsByEntity = append(
				tmpResult[variable].ObservationsByEntity,
				entityObservations,
			)
		}
	}
	for _, variable := range variables {
		if obs, ok := tmpResult[variable]; ok {
			result.ObservationsByVariable = append(result.ObservationsByVariable, obs)
		}
	}
	return result, nil
}
