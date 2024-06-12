// Copyright 2024 Google LLC
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
	"net/http"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/cache"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/store"
)

// Computes a calculation for a variable and entity, based on a formula and input data.
func Calculate(
	ctx context.Context,
	store *store.Store,
	cachedata *cache.Cache,
	metadata *resource.Metadata,
	httpClient *http.Client,
	formula string,
	variable string,
	entity *pbv2.DcidOrExpression,
	inputReq *pbv2.ObservationRequest,
) (*pbv2.ObservationResponse, error) {
	variableFormula, err := NewVariableFormula(formula)
	if err != nil {
		return nil, err
	}
	newReq := &pbv2.ObservationRequest{
		Variable: &pbv2.DcidOrExpression{Dcids: variableFormula.StatVars},
		Entity:   entity,
		Date:     inputReq.GetDate(),
		Value:    inputReq.GetValue(),
		Filter:   inputReq.GetFilter(),
		Select:   inputReq.GetSelect(),
	}
	initialObs, err := ObservationInternal(ctx, store, cachedata, metadata, httpClient, newReq)
	if err != nil {
		return nil, err
	}
	candidateObs := computeLeafObs(initialObs, variableFormula)
	calculatedResp, err := evalExpr(variableFormula.Expr, candidateObs)
	if err != nil {
		return nil, err
	}
	newResp := &pbv2.ObservationResponse{
		ByVariable: map[string]*pbv2.VariableObservation{
			variable: {},
		},
		Facets: map[string]*pb.Facet{},
	}
	for entity, entityData := range calculatedResp {
		facetObs := []*pbv2.FacetObservation{}
		for facetId, facetData := range entityData {
			if _, ok := newResp.Facets[facetId]; !ok {
				newResp.Facets[facetId] = initialObs.Facets[facetId]
			}
			facetObs = append(facetObs, &pbv2.FacetObservation{
				FacetId:      facetId,
				Observations: facetData,
				EarliestDate: facetData[0].GetDate(),
				LatestDate:   facetData[len(facetData)-1].GetDate(),
				ObsCount:     int32(len(facetData)),
			})
		}
		if len(newResp.ByVariable[variable].ByEntity) == 0 {
			newResp.ByVariable[variable].ByEntity = map[string]*pbv2.EntityObservation{}
		}
		newResp.ByVariable[variable].ByEntity[entity] = &pbv2.EntityObservation{
			OrderedFacets: facetObs,
		}
	}
	return newResp, nil
}

// Detects holes in a V2ObservationResponse and attempts to fill them using calculations.
func MaybeCalculateHoles(
	ctx context.Context,
	store *store.Store,
	cachedata *cache.Cache,
	metadata *resource.Metadata,
	httpClient *http.Client,
	inputReq *pbv2.ObservationRequest,
	inputResp *pbv2.ObservationResponse,
) ([]*pbv2.ObservationResponse, error) {
	result := []*pbv2.ObservationResponse{}
	holes, err := findObservationResponseHoles(inputReq, inputResp)
	if err != nil {
		return nil, err
	}
	for variable, entity := range holes {
		formulas, ok := cachedata.SVFormula()[variable]
		if !ok {
			continue
		}
		currentEntity := entity
		for _, formula := range formulas {
			calculatedResp, err := Calculate(
				ctx,
				store,
				cachedata,
				metadata,
				httpClient,
				formula,
				variable,
				currentEntity,
				inputReq,
			)
			if err != nil {
				return nil, err
			}
			variableObs := calculatedResp.ByVariable[variable]
			if entity.GetExpression() != "" {
				if len(variableObs.ByEntity) > 0 {
					result = append(result, calculatedResp)
					break
				}
			} else if len(entity.GetDcids()) != 0 {
				newEntityDcids := []string{}
				for _, dcid := range entity.GetDcids() {
					if _, ok := variableObs.ByEntity[dcid]; !ok {
						newEntityDcids = append(newEntityDcids, dcid)
					}
				}
				if len(newEntityDcids) < len(currentEntity.GetDcids()) {
					result = append(result, calculatedResp)
					if len(newEntityDcids) == 0 {
						break
					}
				}
				// Still some empty dcids, so try next formula.
				currentEntity = &pbv2.DcidOrExpression{Dcids: newEntityDcids}
			}
		}
	}
	return result, nil
}
