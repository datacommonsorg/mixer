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
	"log/slog"
	"net/http"
	"sort"

	"github.com/datacommonsorg/mixer/internal/merger"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/ranking"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	observationhelper "github.com/datacommonsorg/mixer/internal/server/v2/observation/helper"
	"github.com/datacommonsorg/mixer/internal/server/v2/shared"
	"github.com/datacommonsorg/mixer/internal/sqldb"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/datacommonsorg/mixer/internal/store"
)

// FetchContainedIn fetches data for child places contained in an ancestor place.
func FetchContainedIn(
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
	filter *pbv2.FacetFilter,
) (*pbv2.ObservationResponse, error) {
	// Need to use child places to for direct fetch.
	var result *pbv2.ObservationResponse
	var childPlaces []string
	var err error
	if store.BtGroup != nil {
		readCollectionCache := false
		if queryDate != "" {
			result = &pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{},
				Facets:     map[string]*pb.Facet{},
				PlaceTypes: []string{},
			}
			readCollectionCache = util.HasCollectionCache(ancestor, childType)
			if readCollectionCache {
				var btData map[string]*pb.ObsCollection
				var err error
				btData, err = stat.ReadStatCollection(
					ctx, store.BtGroup, bigtable.BtObsCollection,
					ancestor, childType, variables, queryDate,
				)
				if err != nil {
					return nil, err
				}
				for _, variable := range variables {
					result.ByVariable[variable] = &pbv2.VariableObservation{
						ByEntity: map[string]*pbv2.EntityObservation{},
					}
					// Create a short alias
					obsByEntity := result.ByVariable[variable].ByEntity
					data, ok := btData[variable]
					if !ok || data == nil {
						continue
					}
					cohorts := data.SourceCohorts
					// Sort cohort first, so the preferred source is populated first.
					sort.Sort(ranking.CohortByRank(cohorts))
					for _, cohort := range cohorts {
						facet := util.GetFacet(cohort)
						// If there is a facet filter, check that the cohort matches the
						// filter. Otherwise, skip.
						if !util.ShouldIncludeFacet(filter, facet, "" /*facetId*/) {
							continue
						}
						facetID := util.GetFacetID(facet)
						for entity, val := range cohort.Val {
							if _, ok := obsByEntity[entity]; !ok {
								obsByEntity[entity] = &pbv2.EntityObservation{}
							}
							// When date is in the request, response date is the given date.
							// Otherwise, response date is the latest date from the cache.
							respDate := queryDate
							if respDate == shared.LATEST {
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
					// Gathering all place types for all entities in the collection.
					placeTypesByDcid := data.PlaceDcidToTypes
					placeTypesMap := make(map[string]struct{})
					for entity, placeTypes := range placeTypesByDcid {
						for _, placeType := range placeTypes.Types {
							if _, ok := placeTypesMap[placeType]; !ok {
								obsByEntity[entity].PlaceTypes = append(obsByEntity[entity].PlaceTypes, placeType)
								placeTypesMap[placeType] = struct{}{}
							}
						}
					}
				}
			}
		}
		if !readCollectionCache {
			childPlaces, err = shared.FetchChildPlaces(
				ctx, store, metadata, httpClient, remoteMixer, ancestor, childType)
			if err != nil {
				return nil, err
			}
			totalSeries := len(variables) * len(childPlaces)
			if totalSeries > shared.MaxSeries {
				return nil, status.Errorf(
					codes.Internal,
					"Stop processing large number of concurrent observation series: %d",
					totalSeries,
				)
			}
			slog.Info("Fetch series cache in contained-in observation query")
			directResp, err := FetchDirectBT(
				ctx,
				store.BtGroup,
				variables,
				childPlaces,
				queryDate,
				filter,
			)
			if err != nil {
				return nil, err
			}
			result = shared.TrimObservationsResponse(directResp)
		}
	}

	// Fetch Data from SQLite database.
	if sqldb.IsConnected(&store.SQLClient) {
		sqlResult, err := observationhelper.FetchSQLContainedIn(
			ctx,
			store,
			metadata,
			sqlProvenances,
			httpClient,
			remoteMixer,
			variables,
			ancestor,
			childType,
			queryDate,
			filter,
			childPlaces,
		)
		if err != nil {
			return nil, err
		}
		// Prefer SQL data over BT data, so put sqlResult first.
		result = merger.MergeObservation(sqlResult, result)
	}
	return result, nil
}
