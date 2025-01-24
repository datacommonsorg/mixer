// Copyright 2025 Google LLC
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
	"log"

	"github.com/datacommonsorg/mixer/internal/merger"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/cache"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
	"github.com/datacommonsorg/mixer/internal/server/statvar/formula"
	v2obs "github.com/datacommonsorg/mixer/internal/server/v2/observation"
)

// CalculationProcessor implements the dispatcher.Processor interface for performing calculations.
type CalculationProcessor struct {
	DataSources *datasources.DataSources
	CacheData   *cache.Cache
}

func (processor *CalculationProcessor) PreProcess(rc *dispatcher.RequestContext) error {
	// Calculation doesn't require preprocessing.
	return nil
}

func (processor *CalculationProcessor) PostProcess(rc *dispatcher.RequestContext) error {
	switch rc.Type {
	case dispatcher.TypeObservation:
		return postProcessObservation(processor, rc)
	default:
		return nil
	}
}

func postProcessObservation(p *CalculationProcessor, rc *dispatcher.RequestContext) error {
	if p.CacheData == nil {
		log.Printf("missing cache in calculation processor")
		return nil
	}

	calculatedResp, err := calculateHoles(p, rc)
	if err != nil {
		return err
	}

	allResp := append([]*pbv2.ObservationResponse{rc.CurrentResponse.(*pbv2.ObservationResponse)}, calculatedResp...)
	rc.CurrentResponse = merger.MergeMultiObservation(allResp)
	return nil
}

// calculate computes a calculation for a variable and entity, based on a formula and input data.
func calculate(p *CalculationProcessor, rc *dispatcher.RequestContext, equation *v2obs.Equation, entity *pbv2.DcidOrExpression) (*pbv2.ObservationResponse, error) {

	// Parse formula.
	variableFormula, err := formula.NewVariableFormula(equation.Formula)
	if err != nil {
		return nil, err
	}
	if len(variableFormula.StatVars) == 0 {
		return nil, fmt.Errorf("formula missing variables")
	}

	// Retrieve input observations.
	curReq := rc.CurrentRequest.(*pbv2.ObservationRequest)
	newReq := &pbv2.ObservationRequest{
		Variable: &pbv2.DcidOrExpression{Dcids: variableFormula.StatVars},
		Entity:   entity,
		Date:     curReq.Date,
		Value:    curReq.Value,
		Filter:   curReq.Filter,
		Select:   curReq.Select,
	}
	inputObs, err := p.DataSources.Observation(rc.Context, newReq)
	if err != nil {
		return nil, err
	}

	// Evaluate formula using input observations.
	return v2obs.EvalExpr(variableFormula, inputObs, equation)
}

// calculateHoles detects holes in a ObservationResponse and attempts to fill them using calculations.
func calculateHoles(p *CalculationProcessor, rc *dispatcher.RequestContext) ([]*pbv2.ObservationResponse, error) {
	curReq, curResp := rc.CurrentRequest.(*pbv2.ObservationRequest), rc.CurrentResponse.(*pbv2.ObservationResponse)
	result := []*pbv2.ObservationResponse{}

	holes := v2obs.FindObservationResponseHoles(curReq, curResp)
	for variable, entity := range holes {
		formulas, ok := p.CacheData.SVFormula()[variable]
		if !ok {
			continue
		}
		currentEntity := entity
		for _, formula := range formulas {
			calculatedResp, err := calculate(p, rc, &v2obs.Equation{Variable: variable, Formula: formula}, currentEntity)
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
