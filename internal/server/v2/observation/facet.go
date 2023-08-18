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

package observation

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
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Gets the obs response given a list of retrieved bt data rows where the data
// of each row is a list of facets.
func GetFacetObsResponse(variables []string, facetBtDataList [][]bigtable.BtRow, variableRowPart int) *pbv2.ObservationResponse {
	// Init response
	result := &pbv2.ObservationResponse{
		ByVariable: map[string]*pbv2.VariableObservation{},
		Facets:     map[string]*pb.Facet{},
	}
	for _, variable := range variables {
		result.ByVariable[variable] = &pbv2.VariableObservation{
			ByEntity: map[string]*pbv2.EntityObservation{},
		}
		// Use empty string entity to hold list of all facets available for the
		// variable.
		result.ByVariable[variable].ByEntity[""] = &pbv2.EntityObservation{}
	}
	// Get the list of facets for each sv
	svToFacetList := map[string][]*pb.Facet{}
	for _, btData := range facetBtDataList {
		for _, row := range btData {
			sv := row.Parts[variableRowPart]
			if _, ok := svToFacetList[sv]; !ok {
				svToFacetList[sv] = []*pb.Facet{}
			}
			svToFacetList[sv] = append(svToFacetList[sv], row.Data.(*pb.Facets).GetFacets()...)
		}
	}
	// Go through each list of facets, sort and remove duplicates, and add to
	// result.
	for sv, facetList := range svToFacetList {
		entityObservation := &pbv2.EntityObservation{}
		sort.Sort(ranking.FacetByRank(facetList))
		seenFacets := map[string]struct{}{}
		for _, facet := range facetList {
			facetID := util.GetFacetID(facet)
			if _, ok := seenFacets[facetID]; ok {
				continue
			}
			seenFacets[facetID] = struct{}{}
			entityObservation.OrderedFacets = append(entityObservation.OrderedFacets, &pbv2.FacetObservation{FacetId: facetID})
			result.Facets[facetID] = facet
		}
		result.ByVariable[sv].ByEntity[""] = entityObservation
	}
	return result
}

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
		readCollectionCache := hasCollectionCache(ancestor, childType)
		if readCollectionCache && queryDate != "" {
			btDataList, err := bigtable.Read(
				ctx,
				store.BtGroup,
				bigtable.BtObsCollectionFacet,
				[][]string{{ancestor}, {childType}, variables, {queryDate}},
				func(jsonRaw []byte) (interface{}, error) {
					var facets pb.Facets
					if err := proto.Unmarshal(jsonRaw, &facets); err != nil {
						return nil, err
					}
					return &facets, nil
				},
			)
			if err != nil {
				return nil, err
			}
			result = GetFacetObsResponse(variables, btDataList, 2)
		} else {
			childPlaces, err := fetchChildPlaces(
				ctx, store, metadata, httpClient, remoteMixer, ancestor, childType)
			if err != nil {
				return nil, err
			}
			totalSeries := len(variables) * len(childPlaces)
			if totalSeries > maxSeries {
				return nil, status.Errorf(
					codes.Internal,
					"Stop processing large number of concurrent observation series: %d",
					totalSeries,
				)
			}
			log.Println("Fetch series cache in contained-in observation query")
			// When date doesn't matter, use SeriesFacet to get the facets for the
			// child places
			if queryDate == "" || queryDate == LATEST {
				return SeriesFacet(ctx, store, cache, variables, childPlaces)
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
				facetList := []*pb.Facet{}
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
								facetList = append(facetList, facet)
								break
							}
						}
					}
				}
				sort.Sort(ranking.FacetByRank(facetList))
				entityObservation := &pbv2.EntityObservation{}
				for _, facet := range facetList {
					facetID := util.GetFacetID(facet)
					entityObservation.OrderedFacets = append(entityObservation.OrderedFacets, &pbv2.FacetObservation{FacetId: facetID})
					result.Facets[facetID] = facet
				}
				// Use empty string entity to hold list of all facets available for the
				// variable.
				result.ByVariable[variable].ByEntity[""] = entityObservation
			}
		}
	}
	return result, nil
}

// SeriesFacet implements logic to get all available facets for each variable
// given a list of variables and entities.
func SeriesFacet(
	ctx context.Context,
	store *store.Store,
	cache *resource.Cache,
	variables []string,
	entities []string,
) (*pbv2.ObservationResponse, error) {
	result := &pbv2.ObservationResponse{
		ByVariable: map[string]*pbv2.VariableObservation{},
		Facets:     map[string]*pb.Facet{},
	}
	if store.BtGroup != nil {
		btDataList, err := bigtable.Read(
			ctx,
			store.BtGroup,
			bigtable.BtObsTimeSeriesFacet,
			[][]string{entities, variables},
			func(jsonRaw []byte) (interface{}, error) {
				var facets pb.Facets
				if err := proto.Unmarshal(jsonRaw, &facets); err != nil {
					return nil, err
				}
				return &facets, nil
			},
		)
		if err != nil {
			return nil, err
		}
		result = GetFacetObsResponse(variables, btDataList, 1)
	}
	return result, nil
}
