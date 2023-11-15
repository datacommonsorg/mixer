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
	"sort"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/ranking"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/sqldb/query"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/proto"
)

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
	for _, variable := range variables {
		result.ByVariable[variable] = &pbv2.VariableObservation{
			ByEntity: map[string]*pbv2.EntityObservation{},
		}
	}
	if store.BtGroup != nil {
		btDataList, err := bigtable.Read(
			ctx,
			store.BtGroup,
			bigtable.BtObsTimeSeriesPlaceVariableFacet,
			[][]string{entities, variables},
			func(jsonRaw []byte) (interface{}, error) {
				var res pb.PlaceVariableFacets
				if err := proto.Unmarshal(jsonRaw, &res); err != nil {
					return nil, err
				}
				return &res, nil
			},
		)
		if err != nil {
			return nil, err
		}
		// map of variable to entity to facetId to facet info from all the bt tables
		varEntityFacets := map[string]map[string]map[string]*pb.PlaceVariableFacet{}
		for _, btData := range btDataList {
			for _, row := range btData {
				entity, variable := row.Parts[0], row.Parts[1]
				if _, ok := varEntityFacets[variable]; !ok {
					varEntityFacets[variable] = map[string]map[string]*pb.PlaceVariableFacet{}
				}
				if _, ok := varEntityFacets[variable][entity]; !ok {
					varEntityFacets[variable][entity] = map[string]*pb.PlaceVariableFacet{}
				}
				facetList := row.Data.(*pb.PlaceVariableFacets).GetPlaceVariableFacets()
				for _, placeVarFacet := range facetList {
					facetID := util.GetFacetID(placeVarFacet.Facet)
					seenPlaceVarFacet, ok := varEntityFacets[variable][entity][facetID]
					// If we've seen this facet already and it has the EarliestDate field,
					// don't override the mapped facet info.
					if ok && seenPlaceVarFacet.EarliestDate != "" {
						continue
					}
					varEntityFacets[variable][entity][facetID] = placeVarFacet
				}
			}
		}
		for _, variable := range variables {
			for _, entity := range entities {
				if _, ok := varEntityFacets[variable]; !ok {
					continue
				}
				if _, ok := varEntityFacets[variable][entity]; !ok {
					continue
				}
				result.ByVariable[variable].ByEntity[entity] = &pbv2.EntityObservation{
					OrderedFacets: []*pbv2.FacetObservation{},
				}
				// Create a short alias
				varEntityData := result.ByVariable[variable].ByEntity[entity]
				facetList := []*pb.PlaceVariableFacet{}
				for _, facet := range varEntityFacets[variable][entity] {
					facetList = append(facetList, facet)
				}
				sort.Sort(ranking.FacetByRank(facetList))
				for _, placeVarFacet := range facetList {
					facetID := util.GetFacetID(placeVarFacet.Facet)
					facetObs := &pbv2.FacetObservation{
						FacetId:      facetID,
						EarliestDate: placeVarFacet.EarliestDate,
						LatestDate:   placeVarFacet.LatestDate,
						ObsCount:     placeVarFacet.ObsCount,
					}
					varEntityData.OrderedFacets = append(
						varEntityData.OrderedFacets,
						facetObs,
					)
					result.Facets[facetID] = placeVarFacet.Facet
				}
			}
		}
	}
	if store.SQLClient != nil {
		observationCount, err := query.CountObservation(store.SQLClient, entities, variables)
		if err != nil {
			return nil, err
		}
		hasData := false
		for v, entityObsCount := range observationCount {
			for e, count := range entityObsCount {
				if count == 0 {
					continue
				}
				hasData = true
				if _, ok := result.ByVariable[v].ByEntity[e]; !ok {
					result.ByVariable[v].ByEntity[e] = &pbv2.EntityObservation{
						OrderedFacets: []*pbv2.FacetObservation{},
					}
				}
				varEntityData := result.ByVariable[v].ByEntity[e]
				varEntityData.OrderedFacets = append(varEntityData.OrderedFacets,
					&pbv2.FacetObservation{
						FacetId: "local",
						Observations: []*pb.PointStat{
							{
								Value: proto.Float64(float64(count)),
							},
						},
					},
				)
			}
		}
		if hasData {
			result.Facets["local"] = &pb.Facet{
				ImportName:    "local",
				ProvenanceUrl: "local",
			}
		}
	}
	return result, nil
}
