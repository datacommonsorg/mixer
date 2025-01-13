// Copyright 2023 Google LLC
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

// Package observation is for V2 observation API
package observation

import (
	"context"
	"sort"

	"github.com/datacommonsorg/mixer/internal/merger"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/ranking"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/server/v2/shared"
	"github.com/datacommonsorg/mixer/internal/sqldb/sqlquery"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/proto"
)

// FetchDirect fetches data from both Bigtable cache and SQLite database.
func FetchDirect(
	ctx context.Context,
	store *store.Store,
	sqlProvenances map[string]*pb.Facet,
	variables []string,
	entities []string,
	queryDate string,
	filter *pbv2.FacetFilter,
) (*pbv2.ObservationResponse, error) {
	btObservation, err := FetchDirectBT(
		ctx,
		store.BtGroup,
		variables,
		entities,
		queryDate,
		filter,
	)
	if err != nil {
		return nil, err
	}
	sqlObservation, err := sqlquery.GetObservations(
		ctx,
		&store.SQLClient,
		sqlProvenances,
		variables,
		entities,
		queryDate,
		filter,
	)
	if err != nil {
		return nil, err
	}
	// Prefer SQL data over BT data, so put sqlObservation first.
	return merger.MergeObservation(sqlObservation, btObservation), nil
}

// FetchDirectBT fetches data from Bigtable cache.
func FetchDirectBT(
	ctx context.Context,
	btGroup *bigtable.Group,
	variables []string,
	entities []string,
	queryDate string,
	filter *pbv2.FacetFilter,
) (*pbv2.ObservationResponse, error) {
	result := &pbv2.ObservationResponse{
		ByVariable: map[string]*pbv2.VariableObservation{},
		Facets:     map[string]*pb.Facet{},
	}
	// Init result
	for _, variable := range variables {
		result.ByVariable[variable] = &pbv2.VariableObservation{
			ByEntity: map[string]*pbv2.EntityObservation{},
		}
		for _, entity := range entities {
			result.ByVariable[variable].ByEntity[entity] = &pbv2.EntityObservation{}
		}
	}
	if btGroup == nil {
		return result, nil
	}
	btData, err := stat.ReadStatsPb(ctx, btGroup, entities, variables)
	if err != nil {
		return result, err
	}
	for _, variable := range variables {
		for _, entity := range entities {
			entityObservation := &pbv2.EntityObservation{}
			series := btData[entity][variable].SourceSeries
			if len(series) > 0 {
				// Sort series by rank
				sort.Sort(ranking.SeriesByRank(series))
				for _, series := range series {
					facet := util.GetFacet(series)
					// If there is a facet filter, check that the series matches the
					// filter. Otherwise, skip.
					if !util.ShouldIncludeFacet(filter, facet) {
						continue
					}
					facetID := util.GetFacetID(facet)
					obsList := []*pb.PointStat{}
					for date, value := range series.Val {
						ps := &pb.PointStat{
							Date:  date,
							Value: proto.Float64(value),
						}
						if queryDate != "" && queryDate != shared.LATEST && queryDate != date {
							continue
						}
						obsList = append(obsList, ps)
					}
					if len(obsList) == 0 {
						continue
					}
					sort.SliceStable(obsList, func(i, j int) bool {
						return obsList[i].Date < obsList[j].Date
					})
					if queryDate == shared.LATEST {
						obsList = obsList[len(obsList)-1:]
						// If there is higher quality series, then do not pick from the inferior
						// facet even it could have more recent data.
						if len(entityObservation.OrderedFacets) > 0 && stat.IsInferiorFacetPb(series) {
							break
						}
					}
					result.Facets[facetID] = facet
					entityObservation.OrderedFacets = append(
						entityObservation.OrderedFacets,
						&pbv2.FacetObservation{
							FacetId:      facetID,
							Observations: obsList,
							ObsCount:     int32(len(obsList)),
							EarliestDate: obsList[0].Date,
							LatestDate:   obsList[len(obsList)-1].Date,
						},
					)
				}
			}
			result.ByVariable[variable].ByEntity[entity] = entityObservation
		}
	}
	return result, nil
}
