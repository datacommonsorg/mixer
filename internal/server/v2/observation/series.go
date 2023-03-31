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

// Package observation is for V2 property values API
package observation

import (
	"context"
	"sort"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/ranking"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/proto"

	"github.com/datacommonsorg/mixer/internal/store"
)

const (
	LATEST = "LATEST"
)

// FetchFromSeries fetches data from observation series cache.
func FetchFromSeries(
	ctx context.Context,
	store *store.Store,
	entities []string,
	variables []string,
	queryDate string,
) (*pbv2.ObservationResponse, error) {
	result := &pbv2.ObservationResponse{
		ObservationsByVariable: map[string]*pbv2.VariableObservation{},
		Facets:                 map[string]*pb.StatMetadata{},
	}
	btData, err := stat.ReadStatsPb(ctx, store.BtGroup, entities, variables)
	if err != nil {
		return result, err
	}
	for _, variable := range variables {
		result.ObservationsByVariable[variable] = &pbv2.VariableObservation{
			ObservationsByEntity: map[string]*pbv2.EntityObservation{},
		}
		for _, entity := range entities {
			series := btData[entity][variable].SourceSeries
			entityObservation := &pbv2.EntityObservation{}
			if len(series) > 0 {
				// Read series from BT cache
				sort.Sort(ranking.SeriesByRank(series))
				for _, series := range series {
					metadata := stat.GetMetadata(series)
					facetID := util.GetMetadataHash(metadata)
					obsList := []*pb.PointStat{}
					for date, value := range series.Val {
						ps := &pb.PointStat{
							Date:  date,
							Value: proto.Float64(value),
						}
						if queryDate != "" && queryDate != LATEST && date != queryDate {
							continue
						}
						obsList = append(obsList, ps)
					}
					sort.SliceStable(obsList, func(i, j int) bool {
						return obsList[i].Date < obsList[j].Date
					})
					if queryDate == LATEST {
						obsList = obsList[len(obsList)-1:]
					}
					if len(obsList) > 0 {
						result.Facets[facetID] = metadata
						entityObservation.OrderedFacetObservations = append(
							entityObservation.OrderedFacetObservations,
							&pbv2.FacetObservation{
								FacetId:      facetID,
								Observations: obsList,
							},
						)
					}
				}
			} else if store.MemDb.HasStatVar(variable) {
				// Read series from in-memory database
				series := store.MemDb.ReadSeries(variable, entity)
				for _, series := range series {
					facetID := util.GetMetadataHash(series.Metadata)
					obsList := []*pb.PointStat{}
					for date, value := range series.Val {
						ps := &pb.PointStat{
							Date:  date,
							Value: proto.Float64(value),
						}
						if queryDate != "" && queryDate != LATEST && date != queryDate {
							continue
						}
						obsList = append(obsList, ps)
					}
					sort.SliceStable(obsList, func(i, j int) bool {
						return obsList[i].Date < obsList[j].Date
					})
					if queryDate == LATEST {
						obsList = obsList[len(obsList)-1:]
					}
					if len(obsList) > 0 {
						result.Facets[facetID] = series.Metadata
						entityObservation.OrderedFacetObservations = append(
							entityObservation.OrderedFacetObservations,
							&pbv2.FacetObservation{
								FacetId:      facetID,
								Observations: obsList,
							},
						)
					}
				}
			}
			result.ObservationsByVariable[variable].ObservationsByEntity[entity] = entityObservation
		}
	}
	return result, nil
}
