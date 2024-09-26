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

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/statvar/formula"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/proto"
)

const (
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

// Combine two ObservationResponses using an operator token.
func evalBinaryExpr(
	x, y *pbv2.ObservationResponse,
	op token.Token,
) (*pbv2.ObservationResponse, error) {
	result := &pbv2.ObservationResponse{
		// Use a placeholder for intermediate responses.
		ByVariable: map[string]*pbv2.VariableObservation{INTERMEDIATE_NODE: {}},
		Facets:     map[string]*pb.Facet{},
	}
	xVariableObs, xOk := x.ByVariable[INTERMEDIATE_NODE]
	yVariableObs, yOk := y.ByVariable[INTERMEDIATE_NODE]
	if !xOk || !yOk {
		return nil, fmt.Errorf("missing intermediate variable in intermediate response")
	}
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

func formatCalculatedResponse(
	resp *pbv2.ObservationResponse,
	equation *Equation,
) error {
	// Replace placeholder by final variable.
	variableObs, ok := resp.ByVariable[INTERMEDIATE_NODE]
	if !ok {
		return fmt.Errorf("missing intermediate variable in intermediate response")
	}
	resp.ByVariable[equation.variable] = variableObs
	delete(resp.ByVariable, INTERMEDIATE_NODE)

	// Update Facets with IsDcAggregate=true.
	newFacets := map[string]*pb.Facet{}
	facetIdMap := map[string]string{}
	for oldFacetId, oldFacet := range resp.Facets {
		newFacet := oldFacet
		newFacet.IsDcAggregate = true
		newFacetId := util.GetFacetID(newFacet)
		newFacets[newFacetId] = newFacet
		facetIdMap[oldFacetId] = newFacetId
	}
	resp.Facets = newFacets
	for _, entityObs := range resp.ByVariable[equation.variable].ByEntity {
		for _, facetObs := range entityObs.OrderedFacets {
			facetObs.FacetId = facetIdMap[facetObs.GetFacetId()]
		}
	}
	return nil
}
