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
	"go/parser"
	"go/token"
	"reflect"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"google.golang.org/protobuf/proto"
)

// The info of a node in the AST tree.
type ASTNode struct {
	StatVar string
	Facet   *pb.Facet
}

type VariableFormula struct {
	Expr ast.Expr
	// Map of leaves in AST tree formula to the corresponding StatVar and Facet.
	// The key is encodeForParse(nodeString), where nodeString contains the StatVar dcid and filters,
	// (for example: "Count_Person[mm=US_Census;p=P1Y]").
	LeafData map[string]*ASTNode
	// List of distinct StatVars in the formula.
	StatVars []string
}

// Golang's AST package is used for parsing the formula, so we need to avoid sensitive tokens for
// AST. For those tokens, we swap them with insensitive tokens before the parsing, then swap them
// back after the parsing.
var (
	encodeForParseTokenMap = map[string]string{
		"dc/":          "_DC_SLASH_",
		"dcAggregate/": "_DC_AGGREGATE_SLASH_",
		"[":            "_LEFT_SQUARE_BRACKET_",
		"]":            "_RIGHT_SQUARE_BRACKET_",
		"=":            "_EQUAL_TO_",
		";":            "_SEMICOLON_",
	}
)

func encodeForParse(s string) string {
	res := s
	for k, v := range encodeForParseTokenMap {
		res = strings.ReplaceAll(res, k, v)
	}
	return res
}

func decodeForParse(s string) string {
	res := s
	for k, v := range encodeForParseTokenMap {
		res = strings.ReplaceAll(res, v, k)
	}
	return res

}

// Parse nodeString, which contains a variable and a set of filters.
// For example: Count_Person[mm=US_Census;p=P1Y].
func parseNode(nodeString string) (*ASTNode, error) {
	res := &ASTNode{}

	if strings.Contains(nodeString, "[") { // With filters.
		if !strings.Contains(nodeString, "]") {
			return nil, fmt.Errorf("missing ]")
		}

		leftBracketIndex := strings.Index(nodeString, "[")

		res.Facet = &pb.Facet{}
		filterString := nodeString[leftBracketIndex+1 : len(nodeString)-1]
		for _, filter := range strings.Split(filterString, ";") {
			filterType := filter[0:2]
			filterVal := filter[3:]
			switch filterType {
			case "mm":
				res.Facet.MeasurementMethod = filterVal
			case "op":
				res.Facet.ObservationPeriod = filterVal
			case "ut":
				res.Facet.Unit = filterVal
			case "sf":
				res.Facet.ScalingFactor = filterVal
			default:
				return nil, fmt.Errorf("unsupported filter type: %s", filterType)
			}
		}

		res.StatVar = nodeString[0:leftBracketIndex]

	} else { // No filters.
		res.StatVar = nodeString
	}

	return res, nil
}

func NewVariableFormula(formula string) (*VariableFormula, error) {
	expr, err := parser.ParseExpr(encodeForParse(formula))
	if err != nil {
		return nil, err
	}

	c := &VariableFormula{Expr: expr, LeafData: map[string]*ASTNode{}}
	if err := processNodeInfo(expr, c); err != nil {
		return nil, err
	}

	statVarSet := map[string]struct{}{}
	for k := range c.LeafData {
		statVarSet[c.LeafData[k].StatVar] = struct{}{}
	}
	statVars := []string{}
	for k := range statVarSet {
		statVars = append(statVars, k)
	}
	c.StatVars = statVars

	return c, nil
}

// Recursively iterate through the AST tree, extract and parse nodeString, then fill nodeData.
func processNodeInfo(node ast.Node, c *VariableFormula) error {
	switch t := node.(type) {
	case *ast.BinaryExpr:
		for _, node := range []ast.Node{t.X, t.Y} {
			if reflect.TypeOf(node).String() == "*ast.Ident" {
				nodeString := node.(*ast.Ident).Name
				nodeData, err := parseNode(decodeForParse(nodeString))
				if err != nil {
					return err
				}
				c.LeafData[nodeString] = nodeData
			} else {
				if err := processNodeInfo(node, c); err != nil {
					return err
				}
			}
		}
	case *ast.ParenExpr:
		return processNodeInfo(t.X, c)
	default:
		return fmt.Errorf("unsupported AST type %T", t)
	}

	return nil
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

// Returns a filtered ObservationResponse containing obs that match an ASTNode StatVar and Facet.
func filterObsByASTNode(
	fullResp *pbv2.ObservationResponse,
	node *ASTNode,
) *pbv2.ObservationResponse {
	result := &pbv2.ObservationResponse{
		ByVariable: map[string]*pbv2.VariableObservation{
			node.StatVar: {},
		},
		Facets: map[string]*pb.Facet{},
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
			if len(result.ByVariable[node.StatVar].ByEntity) == 0 {
				result.ByVariable[node.StatVar].ByEntity = map[string]*pbv2.EntityObservation{}
			}
			result.ByVariable[node.StatVar].ByEntity[entity] = &pbv2.EntityObservation{
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
		// Intermediate responses have no StatVar, so use a placeholder.
		ByVariable: map[string]*pbv2.VariableObservation{"": {}},
		Facets:     map[string]*pb.Facet{},
	}
	if len(x.ByVariable) != 1 || len(y.ByVariable) != 1 {
		return nil, fmt.Errorf("more than one variable in intermediate response")
	}
	for _, xVariableObs := range x.ByVariable {
		for _, yVariableObs := range y.ByVariable {
			for entity, xEntityObs := range xVariableObs.ByEntity {
				yEntityObs, ok := yVariableObs.ByEntity[entity]
				if !ok {
					continue
				}
				newOrderedFacets := []*pbv2.FacetObservation{}
				for i := 0; i < len(xEntityObs.OrderedFacets); i++ {
					for j := 0; j < len(yEntityObs.OrderedFacets); j++ {
						if xEntityObs.OrderedFacets[i].GetFacetId() == yEntityObs.OrderedFacets[j].GetFacetId() {
							newFacetId := xEntityObs.OrderedFacets[i].GetFacetId()
							newPointStat, err := mergePointStat(
								xEntityObs.OrderedFacets[i].Observations,
								yEntityObs.OrderedFacets[j].Observations,
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
									result.Facets[newFacetId] = x.Facets[newFacetId]
								}
							}
						}
					}
				}
				if len(newOrderedFacets) > 0 {
					if len(result.ByVariable[""].ByEntity) == 0 {
						result.ByVariable[""].ByEntity = map[string]*pbv2.EntityObservation{}
					}
					result.ByVariable[""].ByEntity[entity] = &pbv2.EntityObservation{
						OrderedFacets: newOrderedFacets,
					}
				}
			}
		}
	}
	return result, nil
}

// Recursively iterate through the AST and perform the calculation.
func evalExpr(
	node ast.Node,
	leafData map[string]*ASTNode,
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
