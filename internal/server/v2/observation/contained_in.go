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

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/placein"
	"github.com/datacommonsorg/mixer/internal/server/ranking"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/proto"

	"github.com/datacommonsorg/mixer/internal/store"
)

// FetchContainedIn fetches data for child places contained in ancestor place.
func FetchContainedIn(
	ctx context.Context,
	store *store.Store,
	variables []string,
	ancestor string,
	childType string,
	queryDate string,
) (*pbv2.ObservationResponse, error) {
	btData := map[string]*pb.ObsCollection{}
	var err error
	// Only query Collection BT table when date is set. Otherwise leave btData
	// empty so later on this queries the Series BT table.
	if queryDate != "" {
		btData, err = stat.ReadStatCollection(
			ctx, store.BtGroup, bigtable.BtObsCollection,
			ancestor, childType, variables, queryDate,
		)
		if err != nil {
			return nil, err
		}
	}
	result := &pbv2.ObservationResponse{
		ByVariable: map[string]*pbv2.VariableObservation{},
		Facets:     map[string]*pb.Facet{},
	}
	variablesMissingData := []string{}
	for _, variable := range variables {
		result.ByVariable[variable] = &pbv2.VariableObservation{
			ByEntity: map[string]*pbv2.EntityObservation{},
		}
		// Create a short alias
		obsByEntity := result.ByVariable[variable].ByEntity
		data, ok := btData[variable]
		if !ok || data == nil {
			variablesMissingData = append(variablesMissingData, variable)
			continue
		}
		cohorts := data.SourceCohorts
		// Sort cohort first, so the preferred source is populated first.
		sort.Sort(ranking.CohortByRank(cohorts))
		for _, cohort := range cohorts {
			facet := util.GetFacet(cohort)
			facetID := util.GetFacetID(facet)
			for entity, val := range cohort.Val {
				if _, ok := obsByEntity[entity]; !ok {
					obsByEntity[entity] = &pbv2.EntityObservation{}
				}
				// When date is in the request, response date is the given date.
				// Otherwise, response date is the latest date from the cache.
				respDate := queryDate
				if respDate == LATEST {
					respDate = cohort.PlaceToLatestDate[entity]
				}
				// If there is higher quality facet, then do not pick from the inferior
				// facet even it could have more recent data.
				if len(obsByEntity[entity].OrderedFacets) > 0 && stat.IsInferiorFacetPb(cohort) {
					continue
				}
				obsByEntity[entity].OrderedFacets = append(
					obsByEntity[entity].OrderedFacets,
					&pbv2.FacetObservation{
						FacetId: facetID,
						Observations: []*pb.PointStat{
							{
								Date:  respDate,
								Value: proto.Float64(val),
							},
						},
					},
				)
				result.Facets[facetID] = facet
			}
		}
	}
	// Check if need to read from memory database.
	for _, variable := range variables {
		if store.MemDb.HasStatVar(variable) {
			variablesMissingData = append(variablesMissingData, variable)
		}
	}
	// Fetch linked places if need to read data from memdb or time series Bigtable
	// cache.
	var childPlaces []string
	if len(variablesMissingData) > 0 {
		// TODO(shifucun): use V2 API
		childPlacesMap, err := placein.GetPlacesIn(
			ctx, store, []string{ancestor}, childType)
		if err != nil {
			return nil, err
		}
		childPlaces = childPlacesMap[ancestor]
	}
	if len(variablesMissingData) > 0 {
		moreResult, err := FetchDirect(
			ctx,
			store,
			variablesMissingData,
			childPlaces,
			queryDate,
		)
		if err != nil {
			return nil, err
		}
		for variable, variableData := range moreResult.ByVariable {
			for entity, entityData := range variableData.ByEntity {
				if len(entityData.OrderedFacets) == 0 {
					delete(variableData.ByEntity, entity)
				}
			}
			result.ByVariable[variable] = variableData
		}
		for facet, res := range moreResult.Facets {
			result.Facets[facet] = res
		}
	}
	return result, nil
}
