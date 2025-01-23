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

package observation

import (
	"context"
	"fmt"

	"github.com/datacommonsorg/mixer/internal/merger"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/cache"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/statvar/formula"
	v2obs "github.com/datacommonsorg/mixer/internal/server/v2/observation"
)

// calculate computes a calculation for a variable and entity, based on a formula and input data.
func calculate(ctx context.Context, ds *datasources.DataSources, equation *v2obs.Equation, entity *pbv2.DcidOrExpression, req *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {

	// Parse formula.
	variableFormula, err := formula.NewVariableFormula(equation.Formula)
	if err != nil {
		return nil, err
	}
	if len(variableFormula.StatVars) == 0 {
		return nil, fmt.Errorf("formula missing variables")
	}

	// Retrieve input observations.
	newReq := &pbv2.ObservationRequest{
		Variable: &pbv2.DcidOrExpression{Dcids: variableFormula.StatVars},
		Entity:   entity,
		Date:     req.Date,
		Value:    req.Value,
		Filter:   req.Filter,
		Select:   req.Select,
	}
	inputObs, err := ds.Observation(ctx, newReq)
	if err != nil {
		return nil, err
	}

	// Evalutate formula using input observations.
	return v2obs.EvalExpr(variableFormula, inputObs, equation)
}

// calculateHoles detects holes in a ObservationResponse and attempts to fill them using calculations.
func calculateHoles(ctx context.Context, ds *datasources.DataSources, cachedata *cache.Cache, req *pbv2.ObservationRequest, initialResp *pbv2.ObservationResponse) ([]*pbv2.ObservationResponse, error) {
	result := []*pbv2.ObservationResponse{}

	holes := v2obs.FindObservationResponseHoles(req, initialResp)
	for variable, entity := range holes {
		formulas, ok := cachedata.SVFormula()[variable]
		if !ok {
			continue
		}
		currentEntity := entity
		for _, formula := range formulas {
			calculatedResp, err := calculate(ctx, ds, &v2obs.Equation{Variable: variable, Formula: formula}, currentEntity, req)
			if err != nil {
				return nil, err
			}

			// Evaluate calculated response to check if there are still holes.
			variableObs := calculatedResp.ByVariable[variable]
			if entity.Expression != "" {
				if len(variableObs.ByEntity) > 0 {
					result = append(result, calculatedResp)
					break
				}
			} else {
				newEntityDcids := []string{}
				for _, dcid := range entity.Dcids {
					if _, ok := variableObs.ByEntity[dcid]; !ok {
						newEntityDcids = append(newEntityDcids, dcid)
					}
				}
				if len(newEntityDcids) < len(currentEntity.Dcids) {
					result = append(result, calculatedResp)
					if len(newEntityDcids) == 0 {
						break
					}
				}
				currentEntity = &pbv2.DcidOrExpression{Dcids: newEntityDcids}
			}
		}
	}
	return result, nil
}

func CalculateObservationResponse(ctx context.Context, ds *datasources.DataSources, cachedata *cache.Cache, req *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
	initialResp, err := ds.Observation(ctx, req)
	if err != nil {
		return nil, err
	}

	calculatedResp, err := calculateHoles(ctx, ds, cachedata, req, initialResp)
	if err != nil {
		return nil, err
	}

	allResp := append([]*pbv2.ObservationResponse{initialResp}, calculatedResp...)
	return merger.MergeMultiObservation(allResp), nil
}
