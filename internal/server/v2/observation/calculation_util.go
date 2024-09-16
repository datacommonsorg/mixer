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
	"go/ast"
	"go/token"
	"strconv"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/statvar/formula"
	"google.golang.org/protobuf/proto"
)

const (
	CONSTANT_ENTITY   = "CONSTANT_ENTITY"
	CONSTANT_NODE     = "CONSTANT_NODE"
	INTERMEDIATE_NODE = "INTERMEDIATE_NODE"
)

// Given an input ObservationResponse, generate a map of variable -> entities with missing data.
func findObservationResponseHoles(
	inputReq *pbv2.ObservationRequest,
	inputResp *pbv2.ObservationResponse,
) (map[string]*pbv2.DcidOrExpression, error) {
	result := map[string]*pbv2.DcidOrExpression{}
	// Formula variables are handled by DerivedSeries.
	if inputReq.Variable.GetFormula() != "" {
		return result, nil
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

func compareFacet(facet1, facet2 *pb.Facet) bool {
	if facet1.GetMeasurementMethod() != facet2.GetMeasurementMethod() {
		return false
	}
	if facet1.GetObservationPeriod() != facet2.GetObservationPeriod() {
		return false
	}
	if facet1.GetUnit() != facet2.GetUnit() {
		return false
	}
	if facet1.GetScalingFactor() != facet2.GetScalingFactor() {
		return false
	}
	return true
}

// Returns a filtered ObservationResponse containing obs that match an ASTNode StatVar and Facet.
func filterObsByASTNode(
	fullResp *pbv2.ObservationResponse,
	node *formula.ASTNode,
) *pbv2.ObservationResponse {
	result := &pbv2.ObservationResponse{
		// Use a placeholder for intermediate responses.
		ByVariable: map[string]*pbv2.VariableObservation{INTERMEDIATE_NODE: {}},
		Facets:     map[string]*pb.Facet{},
	}
	variableObs, ok := fullResp.ByVariable[node.StatVar]
	if !ok {
		return result
	}
	for entity, entityObs := range variableObs.ByEntity {
		filteredFacetObs := []*pbv2.FacetObservation{}
		for _, facetObs := range entityObs.OrderedFacets {
			if node.Facet == nil || compareFacet(node.Facet, fullResp.Facets[facetObs.FacetId]) {
				filteredFacetObs = append(filteredFacetObs, facetObs)
				if _, ok := result.Facets[facetObs.FacetId]; !ok {
					result.Facets[facetObs.FacetId] = fullResp.Facets[facetObs.FacetId]
				}
			}
		}
		if len(filteredFacetObs) > 0 {
			if len(result.ByVariable[INTERMEDIATE_NODE].ByEntity) == 0 {
				result.ByVariable[INTERMEDIATE_NODE].ByEntity = map[string]*pbv2.EntityObservation{}
			}
			result.ByVariable[INTERMEDIATE_NODE].ByEntity[entity] = &pbv2.EntityObservation{
				OrderedFacets: filteredFacetObs,
			}
		}
	}
	return result
}

// Returns an ObservationResponse wrapper around BasicLit.Value.
func generateBasicLitObservationResponse(
	data *ast.BasicLit,
) (*pbv2.ObservationResponse, error) {
	val, err := strconv.ParseFloat(data.Value, 64)
	if err != nil {
		return nil, err
	}
	return &pbv2.ObservationResponse{
		// Use a placeholder for constant responses.
		ByVariable: map[string]*pbv2.VariableObservation{CONSTANT_NODE: {
			ByEntity: map[string]*pbv2.EntityObservation{CONSTANT_ENTITY: {
				OrderedFacets: []*pbv2.FacetObservation{{
					Observations: []*pb.PointStat{{
						Value: &val,
					}},
				}},
			}},
		}},
	}, nil
}

// Combine two PointStat series using an operator token.
func mergePointStat(
	x, y []*pb.PointStat,
	op token.Token,
) ([]*pb.PointStat, error) {
	result := []*pb.PointStat{}
	xIdx, yIdx := 0, 0
	for xIdx < len(x) && yIdx < len(y) {
		xDate, yDate := x[xIdx].GetDate(), y[yIdx].GetDate()
		if xDate < yDate {
			xIdx++
		} else if yDate < xDate {
			yIdx++
		} else {
			xVal := x[xIdx].GetValue()
			yVal := y[yIdx].GetValue()
			var val float64
			switch op {
			case token.ADD:
				val = xVal + yVal
			case token.SUB:
				val = xVal - yVal
			case token.MUL:
				val = xVal * yVal
			case token.QUO:
				if yVal == 0 {
					return nil, fmt.Errorf("denominator cannot be zero")
				}
				val = xVal / yVal
			default:
				return nil, fmt.Errorf("unsupported op (token) %v", op)
			}
			result = append(result, &pb.PointStat{
				Date:  xDate,
				Value: proto.Float64(val),
			})
			xIdx++
			yIdx++
		}
	}
	return result, nil
}

// Combine one INTERMEDIATE_NODE with one CONSTANT_NODE.
func evalBinaryConstantNodeExpr(
	x, y *pbv2.ObservationResponse,
	op token.Token,
) (*pbv2.ObservationResponse, error) {
	xEntityObs, xOk := x.ByVariable[CONSTANT_NODE].ByEntity[CONSTANT_ENTITY]
	yEntityObs, yOk := y.ByVariable[CONSTANT_NODE].ByEntity[CONSTANT_ENTITY]
	if !xOk || !yOk {
		return nil, fmt.Errorf("missing constant entity in constant response")
	}
	if len(xEntityObs.OrderedFacets) == 0 || len(yEntityObs.OrderedFacets) == 0 || len(xEntityObs.OrderedFacets[0].Observations) == 0 || len(yEntityObs.OrderedFacets[0].Observations) == 0 {
		return nil, fmt.Errorf("missing observations in constant response")
	}
	xVal := xEntityObs.OrderedFacets[0].Observations[0].GetValue()
	yVal := yEntityObs.OrderedFacets[0].Observations[0].GetValue()
	var val float64
	switch op {
	case token.ADD:
		val = xVal + yVal
	case token.SUB:
		val = xVal - yVal
	case token.MUL:
		val = xVal * yVal
	case token.QUO:
		if yVal == 0 {
			return nil, fmt.Errorf("denominator cannot be zero")
		}
		val = xVal / yVal
	default:
		return nil, fmt.Errorf("unsupported op (token) %v", op)
	}
	return &pbv2.ObservationResponse{
		// Use a placeholder for constant responses.
		ByVariable: map[string]*pbv2.VariableObservation{CONSTANT_NODE: {
			ByEntity: map[string]*pbv2.EntityObservation{CONSTANT_ENTITY: {
				OrderedFacets: []*pbv2.FacetObservation{{
					Observations: []*pb.PointStat{{
						Value: &val,
					}},
				}},
			}},
		}},
	}, nil
}

// Combine one INTERMEDIATE_NODE with one CONSTANT_NODE.
func evalBinaryIntermediateConstantNodeExpr(
	intermediate, constant *pbv2.ObservationResponse,
	iFirst bool, // Whether the intermediate response is the first response in the expression.
	op token.Token,
) (*pbv2.ObservationResponse, error) {
	iVariableObs := intermediate.ByVariable[INTERMEDIATE_NODE]
	cEntityObs, ok := constant.ByVariable[CONSTANT_NODE].ByEntity[CONSTANT_ENTITY]
	if !ok {
		return nil, fmt.Errorf("missing constant entity in constant response")
	}
	if len(cEntityObs.OrderedFacets) == 0 || len(cEntityObs.OrderedFacets[0].Observations) == 0 {
		return nil, fmt.Errorf("missing observations in constant response")
	}
	cVal := cEntityObs.OrderedFacets[0].Observations[0].GetValue()
	result := &pbv2.ObservationResponse{
		// Use a placeholder for intermediate responses.
		ByVariable: map[string]*pbv2.VariableObservation{INTERMEDIATE_NODE: {}},
		Facets:     map[string]*pb.Facet{},
	}
	for entity, iEntityObs := range iVariableObs.ByEntity {
		facets := iEntityObs.OrderedFacets
		newOrderedFacets := []*pbv2.FacetObservation{}
		for i := 0; i < len(facets); i++ {
			newFacetId := facets[i].GetFacetId()
			newPointStat := []*pb.PointStat{}
			for _, obs := range facets[i].Observations {
				iVal := obs.GetValue()
				var val float64
				switch op {
				case token.ADD:
					val = iVal + cVal
				case token.SUB:
					if iFirst {
						val = iVal - cVal
					} else {
						val = cVal - iVal
					}
				case token.MUL:
					val = iVal * cVal
				case token.QUO:
					if iFirst {
						if cVal == 0 {
							return nil, fmt.Errorf("denominator cannot be zero")
						}
						val = iVal / cVal
					} else {
						if iVal == 0 {
							return nil, fmt.Errorf("denominator cannot be zero")
						}
						val = cVal / iVal
					}
				default:
					return nil, fmt.Errorf("unsupported op (token) %v", op)
				}
				newPointStat = append(newPointStat, &pb.PointStat{
					Date:  obs.GetDate(),
					Value: proto.Float64(val),
				})
			}
			if len(newPointStat) > 0 {
				newOrderedFacets = append(newOrderedFacets, &pbv2.FacetObservation{
					FacetId:      newFacetId,
					Observations: newPointStat,
					EarliestDate: newPointStat[0].GetDate(),
					LatestDate:   newPointStat[len(newPointStat)-1].GetDate(),
					ObsCount:     int32(len(newPointStat)),
				})
				if _, ok := result.Facets[newFacetId]; !ok {
					// TODO: Determine if calculated facet should be the same as input facet.
					result.Facets[newFacetId] = intermediate.Facets[newFacetId]
				}
			}
		}
		if len(newOrderedFacets) > 0 {
			if len(result.ByVariable[INTERMEDIATE_NODE].ByEntity) == 0 {
				result.ByVariable[INTERMEDIATE_NODE].ByEntity = map[string]*pbv2.EntityObservation{}
			}
			result.ByVariable[INTERMEDIATE_NODE].ByEntity[entity] = &pbv2.EntityObservation{
				OrderedFacets: newOrderedFacets,
			}
		}
	}
	return result, nil
}

// Combine two INTERMEDIATE_NODE ObservationResponses using an operator token.
func evalBinaryIntermediateNodeExpr(
	x, y *pbv2.ObservationResponse,
	op token.Token,
) (*pbv2.ObservationResponse, error) {
	result := &pbv2.ObservationResponse{
		// Use a placeholder for intermediate responses.
		ByVariable: map[string]*pbv2.VariableObservation{INTERMEDIATE_NODE: {}},
		Facets:     map[string]*pb.Facet{},
	}
	xVariableObs := x.ByVariable[INTERMEDIATE_NODE]
	yVariableObs := y.ByVariable[INTERMEDIATE_NODE]
	for entity, xEntityObs := range xVariableObs.ByEntity {
		yEntityObs, ok := yVariableObs.ByEntity[entity]
		if !ok {
			continue
		}
		xFacets := xEntityObs.OrderedFacets
		yFacets := yEntityObs.OrderedFacets
		newOrderedFacets := []*pbv2.FacetObservation{}
		for i := 0; i < len(xFacets); i++ {
			for j := 0; j < len(yFacets); j++ {
				if xFacets[i].GetFacetId() == yFacets[j].GetFacetId() {
					newFacetId := xFacets[i].GetFacetId()
					newPointStat, err := mergePointStat(
						xFacets[i].Observations,
						yFacets[j].Observations,
						op,
					)
					if err != nil {
						return nil, err
					}
					if len(newPointStat) > 0 {
						newOrderedFacets = append(newOrderedFacets, &pbv2.FacetObservation{
							FacetId:      newFacetId,
							Observations: newPointStat,
							EarliestDate: newPointStat[0].GetDate(),
							LatestDate:   newPointStat[len(newPointStat)-1].GetDate(),
							ObsCount:     int32(len(newPointStat)),
						})
						if _, ok := result.Facets[newFacetId]; !ok {
							// TODO: Determine if calculated facet should be the same as input facet.
							result.Facets[newFacetId] = x.Facets[newFacetId]
						}
					}
				}
			}
		}
		if len(newOrderedFacets) > 0 {
			if len(result.ByVariable[INTERMEDIATE_NODE].ByEntity) == 0 {
				result.ByVariable[INTERMEDIATE_NODE].ByEntity = map[string]*pbv2.EntityObservation{}
			}
			result.ByVariable[INTERMEDIATE_NODE].ByEntity[entity] = &pbv2.EntityObservation{
				OrderedFacets: newOrderedFacets,
			}
		}
	}
	return result, nil
}

// Combine two ObservationResponses using an operator token.
func evalBinaryExpr(
	x, y *pbv2.ObservationResponse,
	op token.Token,
) (*pbv2.ObservationResponse, error) {
	_, xIntermediateOk := x.ByVariable[INTERMEDIATE_NODE]
	_, yIntermediateOk := y.ByVariable[INTERMEDIATE_NODE]
	_, xConstantOk := x.ByVariable[CONSTANT_NODE]
	_, yConstantOk := y.ByVariable[CONSTANT_NODE]
	if xIntermediateOk && yIntermediateOk {
		return evalBinaryIntermediateNodeExpr(x, y, op)
	}
	if xIntermediateOk && yConstantOk {
		return evalBinaryIntermediateConstantNodeExpr(x, y, true /*iFirst*/, op)
	}
	if xConstantOk && yIntermediateOk {
		return evalBinaryIntermediateConstantNodeExpr(y, x, false /*iFirst*/, op)
	}
	if xConstantOk && yConstantOk {
		return evalBinaryConstantNodeExpr(x, y, op)
	}
	return nil, fmt.Errorf("missing inputs in intermediate response")
}

// Recursively iterate through the AST and perform the calculation.
func evalExpr(
	node ast.Node,
	leafData map[string]*formula.ASTNode,
	inputResp *pbv2.ObservationResponse,
) (*pbv2.ObservationResponse, error) {
	// If a node is of type *ast.Ident, it is a leaf with an obs value.
	// Otherwise, it might be *ast.ParenExpr or *ast.BinaryExpr, so we continue recursing it to
	// compute the obs value for the subtree..
	switch t := node.(type) {
	case *ast.Ident:
		return filterObsByASTNode(inputResp, leafData[node.(*ast.Ident).Name]), nil
	case *ast.BasicLit:
		basicLit, err := generateBasicLitObservationResponse(t)
		if err != nil {
			return nil, err
		}
		return basicLit, nil
	case *ast.BinaryExpr:
		xObs, err := evalExpr(t.X, leafData, inputResp)
		if err != nil {
			return nil, err
		}
		yObs, err := evalExpr(t.Y, leafData, inputResp)
		if err != nil {
			return nil, err
		}
		return evalBinaryExpr(xObs, yObs, t.Op)
	case *ast.ParenExpr:
		return evalExpr(t.X, leafData, inputResp)
	default:
		return nil, fmt.Errorf("unsupported ast type %T", t)
	}
}
