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

package formula

import (
	"fmt"
	"go/ast"
	"go/parser"
	"reflect"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
)

// The info of a node in the AST tree.
type ASTNode struct {
	StatVar string
	Facet   *pb.Facet
	Value   float32
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
	case *ast.BasicLit:
		// Handle constants when evaluating formula.
	default:
		return fmt.Errorf("unsupported AST type %T", t)
	}

	return nil
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
