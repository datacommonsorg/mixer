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

// API Implementation for /v1/bulk/observations/point/linked

package observations

import (
	"context"
	"sort"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/placein"
	"github.com/datacommonsorg/mixer/internal/server/ranking"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// BulkPointLinked implements API for Mixer.BulkObservationsPointLinked.
func BulkPointLinked(
	ctx context.Context,
	in *pbv1.BulkObservationsPointLinkedRequest,
	store *store.Store,
) (*pbv1.BulkObservationsPointResponse, error) {
	entityType := in.GetEntityType()
	linkedEntity := in.GetLinkedEntity()
	linkedProperty := in.GetLinkedProperty()
	variables := in.GetVariables()
	date := in.GetDate()
	allFacets := in.GetAllFacets()
	if linkedEntity == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"missing required argument: linked_entity")
	}
	if linkedProperty != "containedInPlace" {
		return nil, status.Errorf(codes.InvalidArgument,
			"linked_property can only be 'containedInPlace'")
	}
	if len(variables) == 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"missing required argument: variables")
	}
	if entityType == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"missing required argument: entity_type")
	}
	dateKey := date
	if date == "" {
		dateKey = "LATEST"
	}
	// Read from cache directly
	cacheData, err := stat.ReadStatCollection(
		ctx, store.BtGroup, bigtable.BtObsCollection,
		linkedEntity, entityType, variables, dateKey,
	)
	if err != nil {
		return nil, err
	}
	result := &pbv1.BulkObservationsPointResponse{
		Facets: map[string]*pb.Facet{},
	}

	variablesMissingData := []string{}

	for _, variable := range variables {
		data, ok := cacheData[variable]
		if !ok || data == nil {
			variablesMissingData = append(variablesMissingData, variable)
			continue
		}
		entityResult := map[string]*pbv1.EntityObservations{}
		cohorts := data.SourceCohorts
		// Sort cohort first, so the preferred source is populated first.
		sort.Sort(ranking.CohortByRank(cohorts))
		for _, cohort := range cohorts {
			facet := util.GetFacet(cohort)
			facetID := util.GetFacetID(facet)
			result.Facets[facetID] = facet
			for entity, val := range cohort.Val {
				// When date is in the request, response date is the given date.
				// Otherwise, response date is the latest date from the cache.
				respDate := date
				if respDate == "" {
					respDate = cohort.PlaceToLatestDate[entity]
				}
				if _, ok := entityResult[entity]; !ok {
					entityResult[entity] = &pbv1.EntityObservations{
						Entity:        entity,
						PointsByFacet: []*pb.PointStat{},
					}
				}
				entityResult[entity].PointsByFacet = append(
					entityResult[entity].PointsByFacet,
					&pb.PointStat{
						Date:  respDate,
						Value: proto.Float64(val),
						Facet: facetID,
					},
				)
			}
		}
		variableObservations := &pbv1.VariableObservations{
			Variable: variable,
		}
		allEntities := []string{}
		for entity := range entityResult {
			allEntities = append(allEntities, entity)
		}
		sort.Strings(allEntities)
		for _, entity := range allEntities {
			variableObservations.ObservationsByEntity = append(
				variableObservations.ObservationsByEntity,
				entityResult[entity],
			)
		}
		result.ObservationsByVariable = append(
			result.ObservationsByVariable,
			variableObservations,
		)
	}
	// Check if need to read from memory database.
	variableInMemDb := false
	for _, variable := range variables {
		if store.MemDb.HasStatVar(variable) {
			variableInMemDb = true
			break
		}
	}
	// Fetch linked places if need to read data from memdb or time series Bigtable
	// cache.
	var childPlaces []string
	if len(variablesMissingData) > 0 || variableInMemDb {
		// TODO(shifucun): use V1 API /v1/bulk/property/out/values/linked here
		childPlacesMap, err := placein.GetPlacesIn(
			ctx, store, []string{linkedEntity}, entityType)
		if err != nil {
			return nil, err
		}
		childPlaces = childPlacesMap[linkedEntity]
	}
	// Missing data from ObsCollection cache, fetch stat series for each
	// entity separately.
	if len(variablesMissingData) > 0 {
		moreResult, err := BulkPoint(
			ctx,
			&pbv1.BulkObservationsPointRequest{
				Variables: variablesMissingData,
				Entities:  childPlaces,
				Date:      date,
			},
			store,
		)
		if err != nil {
			return nil, err
		}
		result.ObservationsByVariable = append(
			result.ObservationsByVariable,
			moreResult.ObservationsByVariable...,
		)
		for facet := range moreResult.Facets {
			result.Facets[facet] = moreResult.Facets[facet]
		}
	}
	// Merge data from in-memory database.
	if variableInMemDb {
		for _, variable := range variables {
			if !store.MemDb.HasStatVar(variable) {
				continue
			}
			observationsByEntity := []*pbv1.EntityObservations{}
			for _, entity := range childPlaces {
				pointValue, facet := store.MemDb.ReadPointValue(variable, entity, date)
				// Override public data from private import
				if pointValue != nil {
					facetID := util.GetFacetID(facet)
					pointValue.Facet = facetID
					result.Facets[facetID] = facet
					observationsByEntity = append(
						observationsByEntity,
						&pbv1.EntityObservations{
							Entity:        entity,
							PointsByFacet: []*pb.PointStat{pointValue},
						},
					)
				}
			}
			result.ObservationsByVariable = append(
				result.ObservationsByVariable,
				&pbv1.VariableObservations{
					Variable:             variable,
					ObservationsByEntity: observationsByEntity,
				},
			)
		}
	}
	// Get the preferred facet
	if !allFacets {
		for _, varibleObservation := range result.ObservationsByVariable {
			for _, entityObservation := range varibleObservation.ObservationsByEntity {
				if len(entityObservation.PointsByFacet) == 0 {
					continue
				}
				if date != "" {
					entityObservation.PointsByFacet = entityObservation.PointsByFacet[0:1]
				} else {
					// When observation exists from higher ranked cohort, but the current
					// cohort has later date and is not inferior facet (like wikidata),
					// prefer the current cohort.
					preferredPoint := entityObservation.PointsByFacet[0]
					for _, point := range entityObservation.PointsByFacet {
						if stat.IsInferiorFacet(result.Facets[point.Facet]) {
							break
						}
						if point.Date > preferredPoint.Date {
							preferredPoint = point
						}
					}
					entityObservation.PointsByFacet = []*pb.PointStat{preferredPoint}
				}
			}
		}
	}
	return result, nil
}
