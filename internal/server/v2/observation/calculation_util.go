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
	"fmt"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
)

// Given an input ObservationResponse, generate a map of variable -> entities with missing data.
func findObservationResponseHoles(
	inputReq *pbv2.ObservationRequest,
	inputResp *pbv2.ObservationResponse,
) (map[string]*pbv2.DcidOrExpression, error) {
	result := map[string]*pbv2.DcidOrExpression{}
	if inputReq.Variable.GetFormula() != "" {
		return nil, fmt.Errorf("currently do not support nested formulas")
	}
	for variable, variableObs := range inputResp.ByVariable {
		if len(inputReq.Entity.GetDcids()) > 0 {
			holeEntities := []string{}
			for entity, entityObs := range variableObs.ByEntity {
				if len(entityObs.OrderedFacets) == 0 {
					holeEntities = append(holeEntities, entity)
				}
			}
			if len(holeEntities) > 0 {
				result[variable] = &pbv2.DcidOrExpression{Dcids: holeEntities}
			}
		} else if inputReq.Entity.GetExpression() != "" {
			if len(variableObs.ByEntity) == 0 {
				result[variable] = &pbv2.DcidOrExpression{Expression: inputReq.Entity.Expression}
			}
		}
	}
	return result, nil
}
