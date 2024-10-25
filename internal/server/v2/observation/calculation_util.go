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
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/proto"
)

type intermediateResponse struct {
	variableObs *pbv2.VariableObservation
	constantObs *float64
}

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

// Returns a filtered intermediateResponse containing obs that match an ASTNode StatVar and Facet.
func filterObsByASTNode(
	fullResp *pbv2.ObservationResponse,
	node *formula.ASTNode,
) *intermediateResponse {
	result := &pbv2.VariableObservation{}
	variableObs, ok := fullResp.ByVariable[node.StatVar]
	if !ok {
		return &intermediateResponse{variableObs: result}
	}
	for entity, entityObs := range variableObs.ByEntity {
		filteredFacetObs := []*pbv2.FacetObservation{}
		for _, facetObs := range entityObs.OrderedFacets {
			if node.Facet == nil || compareFacet(node.Facet, fullResp.Facets[facetObs.FacetId]) {
				filteredFacetObs = append(filteredFacetObs, facetObs)
			}
		}
		if len(filteredFacetObs) > 0 {
			if len(result.ByEntity) == 0 {
				result.ByEntity = map[string]*pbv2.EntityObservation{}
			}
			result.ByEntity[entity] = &pbv2.EntityObservation{
				OrderedFacets: filteredFacetObs,
			}
		}
	}
	return &intermediateResponse{variableObs: result}
}

// Evaluate a binary operation.
func evalOp(
	x, y float64,
	op token.Token,
) (float64, error) {
	switch op {
	case token.ADD:
		return x + y, nil
	case token.SUB:
		return x - y, nil
	case token.MUL:
		return x * y, nil
	case token.QUO:
		if y == 0 {
			return 0, fmt.Errorf("denominator cannot be zero")
		}
		return x / y, nil
	default:
		return 0, fmt.Errorf("unsupported op (token) %v", op)
	}
}

// Combine two PointStat series using an operator token.
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
			val, err := evalOp(xVal, yVal, op)
			if err != nil {
				return nil, err
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

// Combine two VariableObservations using an operator token.
func evalBinaryVariableObsExpr(
	x, y *pbv2.VariableObservation,
	op token.Token,
) (*intermediateResponse, error) {
	result := &pbv2.VariableObservation{ByEntity: map[string]*pbv2.EntityObservation{}}
	for entity, xEntityObs := range x.ByEntity {
		yEntityObs, ok := y.ByEntity[entity]
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
					}
				}
			}
		}
		if len(newOrderedFacets) > 0 {
			result.ByEntity[entity] = &pbv2.EntityObservation{
				OrderedFacets: newOrderedFacets,
			}
		}
	}
	return &intermediateResponse{variableObs: result}, nil
}

// Combine one VariableObservation with one constant using an operator token.
func evalBinaryVariableConstantNodeExpr(
	intermediate *pbv2.VariableObservation,
	constant *float64,
	iFirst bool, // Whether the intermediate response is the first response in the expression.
	op token.Token,
) (*intermediateResponse, error) {
	result := &pbv2.VariableObservation{ByEntity: map[string]*pbv2.EntityObservation{}}
	for entity, iEntityObs := range intermediate.ByEntity {
		facets := iEntityObs.OrderedFacets
		newOrderedFacets := []*pbv2.FacetObservation{}
		for i := 0; i < len(facets); i++ {
			newFacetId := facets[i].GetFacetId()
			newPointStat := []*pb.PointStat{}
			for _, obs := range facets[i].Observations {
				iVal := obs.GetValue()
				var val float64
				var err error
				if iFirst {
					val, err = evalOp(iVal, *constant, op)
				} else {
					val, err = evalOp(*constant, iVal, op)
				}
				if err != nil {
					return nil, err
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
			}
		}
		if len(newOrderedFacets) > 0 {
			result.ByEntity[entity] = &pbv2.EntityObservation{
				OrderedFacets: newOrderedFacets,
			}
		}
	}
	return &intermediateResponse{variableObs: result}, nil
}

// Combine two VariableObservations using an operator token.
func evalBinaryExpr(
	x, y *intermediateResponse,
	op token.Token,
) (*intermediateResponse, error) {
	if (x.variableObs != nil) && (y.variableObs != nil) {
		return evalBinaryVariableObsExpr(x.variableObs, y.variableObs, op)
	}
	if (x.variableObs != nil) && (y.constantObs != nil) {
		return evalBinaryVariableConstantNodeExpr(x.variableObs, y.constantObs, true /*iFirst*/, op)
	}
	if (x.constantObs != nil) && (y.variableObs != nil) {
		return evalBinaryVariableConstantNodeExpr(y.variableObs, x.constantObs, false /*iFirst*/, op)
	}
	if (x.constantObs != nil) && (y.constantObs != nil) {
		val, err := evalOp(*x.constantObs, *y.constantObs, op)
		if err != nil {
			return nil, err
		}
		return &intermediateResponse{constantObs: &val}, nil
	}
	return nil, fmt.Errorf("invalid binary expr")
}

// Recursively iterate through the AST and perform the calculation.
func evalExpr(
	node ast.Node,
	leafData map[string]*formula.ASTNode,
	inputResp *pbv2.ObservationResponse,
) (*intermediateResponse, error) {
	// If a node is of type *ast.Ident, it is a leaf with an obs value.
	// Otherwise, it might be *ast.ParenExpr or *ast.BinaryExpr, so we continue recursing it to
	// compute the obs value for the subtree..
	switch t := node.(type) {
	case *ast.Ident:
		return filterObsByASTNode(inputResp, leafData[node.(*ast.Ident).Name]), nil
	case *ast.BasicLit:
		val, err := strconv.ParseFloat(t.Value, 64)
		if err != nil {
			return nil, err
		}
		return &intermediateResponse{constantObs: &val}, nil
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
	variableObs *pbv2.VariableObservation,
	inputFacets map[string]*pb.Facet,
	equation *Equation,
) (*pbv2.ObservationResponse, error) {
	resp := &pbv2.ObservationResponse{
		ByVariable: map[string]*pbv2.VariableObservation{
			equation.variable: variableObs,
		},
		Facets: map[string]*pb.Facet{},
	}

	// Update Facets with IsDcAggregate=true.
	facetIdMap := map[string]string{}
	for _, entityObs := range variableObs.ByEntity {
		for _, facetObs := range entityObs.OrderedFacets {
			oldFacetId := facetObs.GetFacetId()
			newFacetId, ok := facetIdMap[oldFacetId]
			if ok {
				facetObs.FacetId = newFacetId
				continue
			}
			oldFacet, ok := inputFacets[oldFacetId]
			if !ok {
				return nil, fmt.Errorf("missing facet id %s", oldFacetId)
			}
			newFacet := proto.Clone(oldFacet).(*pb.Facet)
			newFacet.IsDcAggregate = true
			newFacetId = util.GetFacetID(newFacet)
			facetObs.FacetId = newFacetId
			resp.Facets[newFacetId] = newFacet
			facetIdMap[oldFacetId] = newFacetId
		}
	}
	return resp, nil
}
