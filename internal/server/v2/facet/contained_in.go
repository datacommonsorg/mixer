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

package facet

import (
	"context"
	"log/slog"
	"net/http"
	"sort"

	"github.com/datacommonsorg/mixer/internal/merger"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/cache"
	"github.com/datacommonsorg/mixer/internal/server/ranking"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	observationhelper "github.com/datacommonsorg/mixer/internal/server/v2/observation/helper"
	"github.com/datacommonsorg/mixer/internal/server/v2/shared"
	"github.com/datacommonsorg/mixer/internal/sqldb"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ContainedInFacet implements logic to get the available facets for child
// places contained in an ancestor place for a list of variables.
func ContainedInFacet(
	ctx context.Context,
	store *store.Store,
	cachedata *cache.Cache,
	metadata *resource.Metadata,
	sqlProvenances map[string]*pb.Facet,
	httpClient *http.Client,
	remoteMixer string,
	variables []string,
	ancestor string,
	childType string,
	queryDate string,
) (*pbv2.ObservationResponse, error) {
	result := &pbv2.ObservationResponse{
		ByVariable: map[string]*pbv2.VariableObservation{},
		Facets:     map[string]*pb.Facet{},
	}
	for _, variable := range variables {
		result.ByVariable[variable] = &pbv2.VariableObservation{
			ByEntity: map[string]*pbv2.EntityObservation{},
		}
		result.ByVariable[variable].ByEntity[""] = &pbv2.EntityObservation{}
	}
	if store.BtGroup != nil {
		err := btContainedInFacet(ctx, store, cachedata, metadata, httpClient, remoteMixer, variables, ancestor, childType, queryDate, result)
		if err != nil {
			return nil, err
		}
	}
	if sqldb.IsConnected(&store.SQLClient) {
		sqlResult, err := sqlContainedInFacet(
			ctx,
			store,
			metadata,
			sqlProvenances,
			httpClient,
			remoteMixer,
			variables,
			ancestor,
			childType,
			queryDate)
		if err != nil {
			return nil, err
		}
		// Prefer SQL data over BT data, so put sqlResult first.
		result = merger.MergeObservation(sqlResult, result)
	}
	return result, nil
}

// btContainedInFacet gets facets from BT and populates the specified result.
func btContainedInFacet(
	ctx context.Context,
	store *store.Store,
	cachedata *cache.Cache,
	metadata *resource.Metadata,
	httpClient *http.Client,
	remoteMixer string,
	variables []string,
	ancestor string,
	childType string,
	queryDate string,
	result *pbv2.ObservationResponse,
) error {
	readCollectionCache := util.HasCollectionCache(ancestor, childType)
	if readCollectionCache && queryDate != "" {
		btDataList, err := bigtable.Read(
			ctx,
			store.BtGroup,
			bigtable.BtObsCollectionPlaceVariableFacet,
			[][]string{{ancestor}, {childType}, variables, {queryDate}},
			func(jsonRaw []byte) (interface{}, error) {
				var placeVarfacets pb.PlaceVariableFacets
				if err := proto.Unmarshal(jsonRaw, &placeVarfacets); err != nil {
					return nil, err
				}
				return &placeVarfacets, nil
			},
		)
		if err != nil {
			return err
		}
		// Get the list of facets for each sv
		svToFacetList := map[string][]*pb.PlaceVariableFacet{}
		for _, btData := range btDataList {
			for _, row := range btData {
				sv := row.Parts[2]
				if _, ok := svToFacetList[sv]; !ok {
					svToFacetList[sv] = []*pb.PlaceVariableFacet{}
				}
				svToFacetList[sv] = append(
					svToFacetList[sv],
					row.Data.(*pb.PlaceVariableFacets).GetPlaceVariableFacets()...,
				)
			}
		}
		// Go through each list of facets, sort and remove duplicates, and add to
		// result.
		for sv, facetList := range svToFacetList {
			entityObservation := &pbv2.EntityObservation{}
			sort.Sort(ranking.FacetByRank(facetList))
			seenFacets := map[string]struct{}{}
			for _, facet := range facetList {
				facetID := util.GetFacetID(facet.Facet)
				if _, ok := seenFacets[facetID]; ok {
					continue
				}
				seenFacets[facetID] = struct{}{}
				// TODO: Add additional ObsCount, EarliestDate, LatestDate information.
				// These fields need to be added to collection cache first.
				entityObservation.OrderedFacets = append(
					entityObservation.OrderedFacets,
					&pbv2.FacetObservation{FacetId: facetID},
				)
				result.Facets[facetID] = facet.Facet
			}
			result.ByVariable[sv].ByEntity[""] = entityObservation
		}
	} else {
		childPlaces, err := shared.FetchChildPlaces(
			ctx, store, metadata, httpClient, remoteMixer, ancestor, childType)
		if err != nil {
			return err
		}
		totalSeries := len(variables) * len(childPlaces)
		if totalSeries > shared.MaxSeries {
			return status.Errorf(
				codes.Internal,
				"Stop processing large number of concurrent observation series: %d",
				totalSeries,
			)
		}
		slog.Info("Fetch series cache in contained-in observation query")
		// When date doesn't matter, use SeriesFacet to get the facets for the
		// child places
		if queryDate == "" || queryDate == shared.LATEST {
			resp, err := SeriesFacet(ctx, store, cachedata, variables, childPlaces)
			if err != nil {
				return err
			}
			for _, entityData := range resp.ByVariable {
				seenFacet := map[string]*pbv2.FacetObservation{}
				orderedFacetId := []string{}
				mergedFacetData := &pbv2.EntityObservation{
					OrderedFacets: []*pbv2.FacetObservation{},
				}
				// Note there are no perfect facet order for all the entities.
				// The order here is only an approximate.
				for _, entity := range childPlaces {
					if facetData, ok := entityData.ByEntity[entity]; ok {
						for _, item := range facetData.OrderedFacets {
							// obsCount is the number of entities with data for this facet
							obsCount := int32(1)
							// earliest date is the earliest date any entities have data for
							// this facet
							earliestDate := item.EarliestDate
							// latest date is the latest date any entities have data for
							// this facet
							latestDate := item.LatestDate
							// if this facet has been seen before, update obsCount,
							// earliestDate, and latestDate accordingly.
							if facetObs, ok := seenFacet[item.FacetId]; ok {
								obsCount += facetObs.ObsCount
								if earliestDate == "" || (facetObs.EarliestDate != "" && facetObs.EarliestDate < earliestDate) {
									earliestDate = facetObs.EarliestDate
								}
								if facetObs.LatestDate > latestDate {
									latestDate = facetObs.LatestDate
								}
							} else {
								orderedFacetId = append(orderedFacetId, item.FacetId)
							}
							seenFacet[item.FacetId] = &pbv2.FacetObservation{
								FacetId:      item.FacetId,
								ObsCount:     obsCount,
								EarliestDate: earliestDate,
								LatestDate:   latestDate,
							}
						}
					}
				}
				for _, facetId := range orderedFacetId {
					mergedFacetData.OrderedFacets = append(
						mergedFacetData.OrderedFacets,
						seenFacet[facetId],
					)
				}
				entityData.ByEntity = map[string]*pbv2.EntityObservation{
					"": mergedFacetData,
				}
			}
			for k, v := range resp.ByVariable {
				result.ByVariable[k] = v
			}
			for k, v := range resp.Facets {
				result.Facets[k] = v
			}
			return nil
		}
		// Otherwise, get all source series and process them to get the facets
		btData, err := stat.ReadStatsPb(ctx, store.BtGroup, childPlaces, variables)
		if err != nil {
			return err
		}
		for _, variable := range variables {
			result.ByVariable[variable] = &pbv2.VariableObservation{
				ByEntity: map[string]*pbv2.EntityObservation{},
			}
			seenFacet := map[string]*pb.PlaceVariableFacet{}
			for _, entity := range childPlaces {
				series := btData[entity][variable].SourceSeries
				for _, series := range series {
					facet := util.GetFacet(series)
					facetID := util.GetFacetID(facet)
					for date := range series.Val {
						if queryDate == date {
							// obsCount is the number of entities with data for this facet
							// for this date.
							obsCount := int32(1)
							if _, ok := seenFacet[facetID]; ok {
								obsCount += seenFacet[facetID].ObsCount
							}
							seenFacet[facetID] = &pb.PlaceVariableFacet{
								Facet:        facet,
								ObsCount:     obsCount,
								EarliestDate: date,
								LatestDate:   date,
							}
							break
						}
					}
				}
			}
			facetList := []*pb.PlaceVariableFacet{}
			for _, placeVarFacet := range seenFacet {
				facetList = append(facetList, placeVarFacet)
			}
			sort.Sort(ranking.FacetByRank(facetList))
			entityObservation := &pbv2.EntityObservation{}
			for _, placeVarFacet := range facetList {
				facetID := util.GetFacetID(placeVarFacet.Facet)
				entityObservation.OrderedFacets = append(entityObservation.OrderedFacets,
					&pbv2.FacetObservation{
						FacetId:      facetID,
						ObsCount:     placeVarFacet.ObsCount,
						EarliestDate: placeVarFacet.EarliestDate,
						LatestDate:   placeVarFacet.LatestDate,
					})
				result.Facets[facetID] = placeVarFacet.Facet
			}
			// Use empty string entity to hold list of all facets available for the
			// variable.
			result.ByVariable[variable].ByEntity[""] = entityObservation
		}
	}
	return nil
}

// sqlContainedInFacet gets facets from SQL.
func sqlContainedInFacet(
	ctx context.Context,
	store *store.Store,
	metadata *resource.Metadata,
	sqlProvenances map[string]*pb.Facet,
	httpClient *http.Client,
	remoteMixer string,
	variables []string,
	ancestor string,
	childType string,
	queryDate string,
) (*pbv2.ObservationResponse, error) {
	response, err := observationhelper.FetchSQLContainedIn(ctx, store, metadata, sqlProvenances,
		httpClient, remoteMixer, variables, ancestor, childType, queryDate, &pbv2.FacetFilter{}, []string{})
	if err != nil {
		return nil, err
	}
	// Put facets under an empty string entity (which is the convention for a facet response).
	for _, variableData := range response.ByVariable {
		seenFacets := map[int]struct{}{}
		facets := []*pbv2.FacetObservation{}

		// Remove all entities and collect all facets.
		for entityID, entityData := range variableData.ByEntity {
			for facetID := range entityData.OrderedFacets {
				if _, ok := seenFacets[facetID]; !ok {
					seenFacets[facetID] = struct{}{}
					facets = append(facets, &pbv2.FacetObservation{
						FacetId: entityData.OrderedFacets[facetID].FacetId,
					})
				}
			}
			delete(variableData.ByEntity, entityID)
		}

		// Use an empty string entity to hold list of all facets available for the variable.
		variableData.ByEntity[""] = &pbv2.EntityObservation{
			OrderedFacets: facets,
		}
	}
	return response, nil
}
