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
	"reflect"
	"sort"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
)

type calculatorSeries struct {
	statMetadata *pb.StatMetadata
	// Sorted by date.
	points []*pb.PointStat
}

// The date key of a time series is concatenation of all sorted dates.
func (s *calculatorSeries) dateKey() string {
	dates := []string{}
	for _, point := range s.points {
		dates = append(dates, point.GetDate())
	}
	return strings.Join(dates, "")
}

type varInfo struct {
	statVar          string
	statMetadata     *pb.StatMetadata
	seriesCandidates []*calculatorSeries
	chosenSeries     *calculatorSeries
}

type calculator struct {
	expr ast.Expr
	// Key is encodeForParse(varName).
	varInfoMap map[string]*varInfo
}

func newCalculator(formula string) (*calculator, error) {
	expr, err := parser.ParseExpr(encodeForParse(formula))
	if err != nil {
		return nil, err
	}

	// Iterate the AST, extract var names (variable with optional filters).
	varNameVisitor := newVarNameVisitor()
	ast.Walk(varNameVisitor, expr)

	varInfoMap := map[string]*varInfo{}
	for varName := range varNameVisitor.varNameSet {
		varInfo, err := parseVarName(decodeForParse(varName))
		if err != nil {
			return nil, err
		}
		varInfoMap[varName] = varInfo
	}

	return &calculator{expr: expr, varInfoMap: varInfoMap}, nil
}

func (c *calculator) statVars() []string {
	statVarSet := map[string]struct{}{}
	for k := range c.varInfoMap {
		statVarSet[c.varInfoMap[k].statVar] = struct{}{}
	}
	statVars := []string{}
	for k := range statVarSet {
		statVars = append(statVars, k)
	}
	return statVars
}

func (c *calculator) calculate(
	entityData map[string]*pb.ObsTimeSeries) (*calculatorSeries, error) {
	if err := c.extractSeriesCandidates(entityData); err != nil {
		return nil, err
	}

	if err := c.chooseSeries(); err != nil {
		return nil, err
	}

	return c.evaluateExpr(c.expr)
}

func (c *calculator) extractSeriesCandidates(
	entityData map[string]*pb.ObsTimeSeries) error {

	for varInfoKey, varInfo := range c.varInfoMap {
		statMetadata := varInfo.statMetadata
		if obsTimeSeries, ok := entityData[varInfo.statVar]; ok {
			for _, sourceSeries := range obsTimeSeries.GetSourceSeries() {
				if m := statMetadata.GetMeasurementMethod(); m != "" {
					if m != sourceSeries.GetMeasurementMethod() {
						continue
					}
				}
				if p := statMetadata.GetObservationPeriod(); p != "" {
					if p != sourceSeries.GetObservationPeriod() {
						continue
					}
				}
				if u := statMetadata.GetUnit(); u != "" {
					if u != sourceSeries.GetUnit() {
						continue
					}
				}
				if s := statMetadata.GetScalingFactor(); s != "" {
					if s != sourceSeries.GetScalingFactor() {
						continue
					}
				}
				varInfo.seriesCandidates = append(varInfo.seriesCandidates,
					toCalculatorSeries(sourceSeries))
			}
			if len(varInfo.seriesCandidates) == 0 {
				return fmt.Errorf("no data for %s",
					decodeForParse(varInfoKey))
			}
		} else {
			return fmt.Errorf("no data for %s",
				decodeForParse(varInfoKey))
		}
	}
	return nil
}

func (c *calculator) chooseSeries() error {
	// Get common date keys across all the varInfos.
	list := [][]string{} // A list of lists of series date keys.
	for _, varInfo := range c.varInfoMap {
		dateKeys := []string{}
		for _, series := range varInfo.seriesCandidates {
			dateKeys = append(dateKeys, series.dateKey())
		}
		list = append(list, dateKeys)
	}
	commonDateKeys := util.StringListIntersection(list)
	if len(commonDateKeys) == 0 {
		return fmt.Errorf("no same date range for input time series sets")
	}

	// Choose the longest date key(s), used for selecting series among seriesCandidates.
	// The longest date key represents the longest coverage of dates for time series.
	longestDateKeySet := map[string]struct{}{}
	maxDateKeyLength := 0
	for _, k := range commonDateKeys {
		if l := len(k); l > maxDateKeyLength {
			for k := range longestDateKeySet {
				delete(longestDateKeySet, k)
			}
			longestDateKeySet[k] = struct{}{}
			maxDateKeyLength = l
		} else if l == maxDateKeyLength {
			longestDateKeySet[k] = struct{}{}
		}
	}

	// Set chosenSeries for each varInfo.
	for _, varInfo := range c.varInfoMap {
		filteredSeriesCandidates := []*calculatorSeries{}
		for _, series := range varInfo.seriesCandidates {
			if _, ok := longestDateKeySet[series.dateKey()]; ok {
				filteredSeriesCandidates = append(filteredSeriesCandidates, series)
			}
		}
		varInfo.chosenSeries = rankCalculatorSeries(filteredSeriesCandidates)
	}

	return nil
}

// Recursively iterate through the AST and compute the result.
//
// The existing ast.Walk() doesn't work here, because in ast.Walk(), visiting children is the last
// step of the function that visits parent, so the information in child nodes cannot be passed
// above in the iteration.
func (c *calculator) evaluateExpr(node ast.Node) (*calculatorSeries, error) {
	// If a node is of type *ast.Ident, it is a leaf with a series value.
	// Otherwise, it might be *ast.ParenExpr or *ast.BinaryExpr, so we continue recursing it to
	// compute the series value for the subtree..
	computeChildSeries := func(node ast.Node) (*calculatorSeries, error) {
		if reflect.TypeOf(node).String() == "*ast.Ident" {
			return c.varInfoMap[node.(*ast.Ident).Name].chosenSeries, nil
		}
		return c.evaluateExpr(node)
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
		return evaluateBinaryExpr(xSeries, ySeries, t.Op)
	case *ast.ParenExpr:
		return c.evaluateExpr(t.X)
	default:
		return nil, fmt.Errorf("unsupported ast type %T", t)
	}
}

// This implements ast.Visitor, used by ast.Walk, which iterates AST with a depth-first order.
// For each ast.BinaryExpr, it extracts the "Name" property of X and Y.
// For example, for an expression "Count_Person - Count_Person_Female[mm=USCensus]", it returns
// {Count_Person, Count_Person_Female[mm=USCensus]}.
type varNameVisitor struct {
	varNameSet map[string]struct{}
}

func newVarNameVisitor() *varNameVisitor {
	return &varNameVisitor{varNameSet: map[string]struct{}{}}
}

func (v *varNameVisitor) Visit(node ast.Node) (w ast.Visitor) {
	switch t := node.(type) {
	case *ast.BinaryExpr:
		if reflect.TypeOf(t.X).String() == "*ast.Ident" {
			v.varNameSet[t.X.(*ast.Ident).Name] = struct{}{}
		}
		if reflect.TypeOf(t.Y).String() == "*ast.Ident" {
			v.varNameSet[t.Y.(*ast.Ident).Name] = struct{}{}
		}
	}
	return v
}

func toCalculatorSeries(sourceSeries *pb.SourceSeries) *calculatorSeries {
	series := &calculatorSeries{
		statMetadata: &pb.StatMetadata{
			MeasurementMethod: sourceSeries.GetMeasurementMethod(),
			ObservationPeriod: sourceSeries.GetObservationPeriod(),
			Unit:              sourceSeries.GetUnit(),
			ScalingFactor:     sourceSeries.GetScalingFactor(),
		},
		points: []*pb.PointStat{},
	}

	var dates []string
	for date := range sourceSeries.GetVal() {
		dates = append(dates, date)
	}
	sort.Strings(dates)
	for _, date := range dates {
		series.points = append(series.points, &pb.PointStat{
			Date:  date,
			Value: sourceSeries.GetVal()[date],
		})
	}

	return series
}

// TODO(spaceenter): Implement better ranking algorithm than simple string comparisons.
//
// The input `seriesCandidates` all have the same dates.
func rankCalculatorSeries(seriesCandidates []*calculatorSeries) *calculatorSeries {
	statMetadataKey := func(statMetadata *pb.StatMetadata) string {
		return strings.Join([]string{
			statMetadata.GetMeasurementMethod(),
			statMetadata.GetObservationPeriod(),
			statMetadata.GetUnit(),
			statMetadata.GetScalingFactor()}, "-")
	}

	var res *calculatorSeries
	var maxKey string
	for _, series := range seriesCandidates {
		key := statMetadataKey(series.statMetadata)
		if maxKey == "" || maxKey < key {
			maxKey = key
			res = series
		}
	}

	return res
}
