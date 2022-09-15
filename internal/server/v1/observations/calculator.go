// Copyright 2022 Google LLC
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

// Calculator.

// Package observations contain code for observations.
package observations

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
)

// A calculable item that belong to a node in the AST tree.
// The item can be a number, a time series, a map from entity to number, etc.
type calcItem interface {
	key() string
}

// The info of a node in the AST tree.
type nodeData struct {
	statVar        string
	statMetadata   *pb.StatMetadata
	candidateItems []calcItem
	chosenItem     calcItem
}

type calculator struct {
	expr ast.Expr
	// Key is encodeForParse(nodeName).
	nodeDataMap map[string]*nodeData
}

func newCalculator(formula string) (*calculator, error) {
	expr, err := parser.ParseExpr(encodeForParse(formula))
	if err != nil {
		return nil, err
	}

	c := &calculator{expr: expr, nodeDataMap: map[string]*nodeData{}}
	if err := c.processNodeInfo(c.expr); err != nil {
		return nil, err
	}

	return c, nil
}

// Recursively iterate through the AST tree, extract and parse nodeName, then fill nodeData.
func (c *calculator) processNodeInfo(node ast.Node) error {
	switch t := node.(type) {
	case *ast.BinaryExpr:
		for _, node := range []ast.Node{t.X, t.Y} {
			if reflect.TypeOf(node).String() == "*ast.Ident" {
				nodeName := node.(*ast.Ident).Name
				nodeData, err := parseNode(decodeForParse(nodeName))
				if err != nil {
					return err
				}
				c.nodeDataMap[nodeName] = nodeData
			} else {
				if err := c.processNodeInfo(node); err != nil {
					return err
				}
			}
		}
	case *ast.ParenExpr:
		return c.processNodeInfo(t.X)
	default:
		return fmt.Errorf("unsupported AST type %T", t)
	}

	return nil
}

func (c *calculator) statVars() []string {
	statVarSet := map[string]struct{}{}
	for k := range c.nodeDataMap {
		statVarSet[c.nodeDataMap[k].statVar] = struct{}{}
	}
	statVars := []string{}
	for k := range statVarSet {
		statVars = append(statVars, k)
	}
	return statVars
}

func (c *calculator) calculate(
	dataMap interface{},
	extractItemCandidates func(btData interface{}, statVar string,
		statMetadata *pb.StatMetadata) ([]calcItem, error),
	evalBinaryExpr func(x, y calcItem, op token.Token) (calcItem, error),
	rankCalcItem func(items []calcItem) calcItem,
) (calcItem, error) {
	if err := c.fillItemCandidates(dataMap, extractSeriesCandidates); err != nil {
		return nil, err
	}

	if err := c.chooseItem(rankCalcItem); err != nil {
		return nil, err
	}

	return c.evalExpr(c.expr, evalBinaryExpr)
}

func (c *calculator) fillItemCandidates(
	btData interface{},
	extractItemCandidates func(
		btData interface{},
		statVar string,
		statMetadata *pb.StatMetadata) ([]calcItem, error),
) error {
	for _, nodeData := range c.nodeDataMap {
		calcItems, err := extractItemCandidates(
			btData, nodeData.statVar, nodeData.statMetadata)
		if err != nil {
			return err
		}
		nodeData.candidateItems = append(nodeData.candidateItems, calcItems...)
	}
	return nil
}

func (c *calculator) chooseItem(
	rankCalcItem func(items []calcItem) calcItem,
) error {
	// Get common date keys across all the varInfos.
	list := [][]string{} // A list of lists of series date keys.
	for _, nodeData := range c.nodeDataMap {
		itemKeys := []string{}
		for _, item := range nodeData.candidateItems {
			itemKeys = append(itemKeys, item.key())
		}
		list = append(list, itemKeys)
	}
	commonItemKeys := util.StringListIntersection(list)
	if len(commonItemKeys) == 0 {
		return fmt.Errorf("no same date range for input time series sets")
	}

	// Choose the longest item key(s), used for selecting series among candidateItems.
	// For time series, the key represents the longest coverage of dates.
	// For obs collection, the key represents the largest set of entities.
	longestItemKeySet := map[string]struct{}{}
	maxItemKeyLength := 0
	for _, k := range commonItemKeys {
		if l := len(k); l > maxItemKeyLength {
			for k := range longestItemKeySet {
				delete(longestItemKeySet, k)
			}
			longestItemKeySet[k] = struct{}{}
			maxItemKeyLength = l
		} else if l == maxItemKeyLength {
			longestItemKeySet[k] = struct{}{}
		}
	}

	// Set chosenItem for each nodeData.
	for _, nodeData := range c.nodeDataMap {
		filteredItemCandidates := []calcItem{}
		for _, item := range nodeData.candidateItems {
			if _, ok := longestItemKeySet[item.key()]; ok {
				filteredItemCandidates = append(filteredItemCandidates, item)
			}
		}
		nodeData.chosenItem = rankCalcItem(filteredItemCandidates)
	}

	return nil
}

// Recursively iterate through the AST and perform the calculation.
func (c *calculator) evalExpr(
	node ast.Node,
	evalBinaryExpr func(x, y calcItem, op token.Token) (calcItem, error),
) (calcItem, error) {
	// If a node is of type *ast.Ident, it is a leaf with a series value.
	// Otherwise, it might be *ast.ParenExpr or *ast.BinaryExpr, so we continue recursing it to
	// compute the series value for the subtree..
	computeChildSeries := func(node ast.Node) (calcItem, error) {
		if reflect.TypeOf(node).String() == "*ast.Ident" {
			return c.nodeDataMap[node.(*ast.Ident).Name].chosenItem, nil
		}
		return c.evalExpr(node, evalBinaryExpr)
	}

	switch t := node.(type) {
	case *ast.BinaryExpr:
		xSeries, err := computeChildSeries(t.X)
		if err != nil {
			return nil, err
		}
		ySeries, err := computeChildSeries(t.Y)
		if err != nil {
			return nil, err
		}
		return evalBinaryExpr(xSeries, ySeries, t.Op)
	case *ast.ParenExpr:
		return c.evalExpr(t.X, evalBinaryExpr)
	default:
		return nil, fmt.Errorf("unsupported ast type %T", t)
	}
}
