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
		for _, btData := range btDataList {
			for _, row := range btData {
				entity, variable := row.Parts[0], row.Parts[1]
				result.ByVariable[variable].ByEntity[entity] = &pbv2.EntityObservation{
					OrderedFacets: []*pbv2.FacetObservation{},
				}
				// Create a short alias
				varEntityData := result.ByVariable[variable].ByEntity[entity]
				facetList := row.Data.(*pb.PlaceVariableFacets).GetPlaceVariableFacets()
				sort.Sort(ranking.FacetByRank(facetList))
				for _, placeVarFacet := range facetList {
					facetID := util.GetFacetID(placeVarFacet.Facet)
					facetObs := &pbv2.FacetObservation{
						FacetId: facetID,
						Observations: []*pb.PointStat{
							{
								Value: proto.Float64(float64(placeVarFacet.ObsCount)),
							},
						},
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
	return result, nil
}
