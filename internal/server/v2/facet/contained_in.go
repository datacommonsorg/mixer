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
	"log"
	"net/http"
	"sort"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/ranking"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/server/v2/observation"
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
	cache *resource.Cache,
	metadata *resource.Metadata,
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
				return nil, err
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
					entityObservation.OrderedFacets = append(
						entityObservation.OrderedFacets,
						&pbv2.FacetObservation{FacetId: facetID},
					)
					result.Facets[facetID] = facet.Facet
				}
				result.ByVariable[sv].ByEntity[""] = entityObservation
			}
		} else {
			childPlaces, err := observation.FetchChildPlaces(
				ctx, store, metadata, httpClient, remoteMixer, ancestor, childType)
			if err != nil {
				return nil, err
			}
			totalSeries := len(variables) * len(childPlaces)
			if totalSeries > observation.MaxSeries {
				return nil, status.Errorf(
					codes.Internal,
					"Stop processing large number of concurrent observation series: %d",
					totalSeries,
				)
			}
			log.Println("Fetch series cache in contained-in observation query")
			// When date doesn't matter, use SeriesFacet to get the facets for the
			// child places
			if queryDate == "" || queryDate == observation.LATEST {
				return SeriesFacet(ctx, store, cache, variables, childPlaces, true)
			}
			// Otherwise, get all source series and process them to get the facets
			btData, err := stat.ReadStatsPb(ctx, store.BtGroup, childPlaces, variables)
			if err != nil {
				return nil, err
			}
			for _, variable := range variables {
				result.ByVariable[variable] = &pbv2.VariableObservation{
					ByEntity: map[string]*pbv2.EntityObservation{},
				}
				seenFacets := map[string]struct{}{}
				facetList := []*pb.PlaceVariableFacet{}
				for _, entity := range childPlaces {
					series := btData[entity][variable].SourceSeries
					for _, series := range series {
						facet := util.GetFacet(series)
						facetID := util.GetFacetID(facet)
						if _, ok := seenFacets[facetID]; ok {
							continue
						}
						for date := range series.Val {
							if queryDate == date {
								seenFacets[facetID] = struct{}{}
								facetList = append(
									facetList,
									&pb.PlaceVariableFacet{Facet: facet, LatestDate: date},
								)
								break
							}
						}
					}
				}
				sort.Sort(ranking.FacetByRank(facetList))
				entityObservation := &pbv2.EntityObservation{}
				for _, placeVarFacet := range facetList {
					facetID := util.GetFacetID(placeVarFacet.Facet)
					entityObservation.OrderedFacets = append(entityObservation.OrderedFacets, &pbv2.FacetObservation{FacetId: facetID})
					result.Facets[facetID] = placeVarFacet.Facet
				}
				// Use empty string entity to hold list of all facets available for the
				// variable.
				result.ByVariable[variable].ByEntity[""] = entityObservation
			}
		}
	}
	return result, nil
}
