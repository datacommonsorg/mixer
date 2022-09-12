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

package observations

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"sort"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/store"
)

// DerivedSeries implements API for Mixer.DerivedObservationsSeries.
func DerivedSeries(
	ctx context.Context,
	in *pb.DerivedObservationsSeriesRequest,
	store *store.Store,
) (*pb.DerivedObservationsSeriesResponse, error) {
	resp := &pb.DerivedObservationsSeriesResponse{}
	entity := in.GetEntity()

	// Parse the formula to extract all the variables, used for reading data from BT.
	varCalculator, err := newVarCalculator(in.GetFormula())
	if err != nil {
		return resp, err
	}
	statVars := varCalculator.statVars()

	// Read data from BT.
	btData, err := stat.ReadStatsPb(
		ctx, store.BtGroup, []string{entity}, statVars)
	if err != nil {
		return resp, err
	}
	entityData, ok := btData[entity]
	if !ok {
		return resp, err
	}

	// Calculate.
	result, err := varCalculator.calculate(entityData)
	if err != nil {
		return resp, err
	}
	for _, p := range result.points {
		resp.Observations = append(resp.Observations, &pb.PointStat{
			Date:  p.GetDate(),
			Value: p.GetValue(),
		})
	}

	return resp, nil
}

type varCalculatorSeries struct {
	statMetadata *pb.StatMetadata
	// Sorted by date.
	points []*pb.PointStat
}

// The date key of a time series is concatenation of all sorted dates.
func (s *varCalculatorSeries) dateKey() string {
	dates := []string{}
	for _, point := range s.points {
		dates = append(dates, point.GetDate())
	}
	return strings.Join(dates, "")
}

type varInfo struct {
	statVar          string
	statMetadata     *pb.StatMetadata
	seriesCandidates []*varCalculatorSeries
	chosenSeries     *varCalculatorSeries
}

type varCalculator struct {
	expr ast.Expr
	// Key is encodeForParse(varName).
	varInfoMap map[string]*varInfo
	debug      bool
}

func newVarCalculator(formula string) (*varCalculator, error) {
	debug := os.Getenv("DEBUG") == "true"

	expr, err := parser.ParseExpr(encodeForParse(formula))
	if err != nil {
		return nil, err
	}

	if debug {
		fs := token.NewFileSet()
		ast.Print(fs, expr)
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

	return &varCalculator{expr: expr, varInfoMap: varInfoMap, debug: debug}, nil
}

func (c *varCalculator) statVars() []string {
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

func (c *varCalculator) calculate(
	entityData map[string]*pb.ObsTimeSeries) (*varCalculatorSeries, error) {
	if err := c.extractSeriesCandidates(entityData); err != nil {
		return nil, err
	}

	if err := c.chooseSeries(); err != nil {
		return nil, err
	}

	return c.evaluateExpr(c.expr)
}

func (c *varCalculator) extractSeriesCandidates(
	entityData map[string]*pb.ObsTimeSeries) error {

	if c.debug {
		fmt.Printf("entityData: %v\n\n", entityData)
		fmt.Printf("varInfoMap: %v\n\n", c.varInfoMap)
	}

	for varInfoKey, varInfo := range c.varInfoMap {
		statMetadata := varInfo.statMetadata

		if c.debug {
			fmt.Printf("varInfo: %v\n\n", varInfo)
			fmt.Printf("statMetadata: %v\n\n", statMetadata)
		}

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
					toVarCalculatorSeries(sourceSeries))
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

func (c *varCalculator) chooseSeries() error {
	// Get common date keys across all the varInfos.
	list := []map[string]struct{}{} // A list of sets of series date keys.
	for _, varInfo := range c.varInfoMap {
		dateKeySet := map[string]struct{}{}
		for _, series := range varInfo.seriesCandidates {
			dateKeySet[series.dateKey()] = struct{}{}
		}
		list = append(list, dateKeySet)
	}
	commonDateKeys := commonStringsAmongStringSets(list)
	if len(commonDateKeys) == 0 {
		return fmt.Errorf("no same date range for input time series sets")
	}

	// Choose the longest date key(s), used for selecting series among seriesCandidates.
	// The longest date key represents the longest coverage of dates for time series.
	longestDateKeySet := map[string]struct{}{}
	maxDateKeyLength := 0
	for k := range commonDateKeys {
		l := len(k)
		if l >= maxDateKeyLength {
			longestDateKeySet[k] = struct{}{}
			if l > maxDateKeyLength {
				maxDateKeyLength = l
			}
		}
	}

	// Set chosenSeries for each varInfo.
	for _, varInfo := range c.varInfoMap {
		filteredSeriesCandidates := []*varCalculatorSeries{}
		for _, series := range varInfo.seriesCandidates {
			if _, ok := longestDateKeySet[series.dateKey()]; ok {
				filteredSeriesCandidates = append(filteredSeriesCandidates, series)
			}
		}
		varInfo.chosenSeries = rankVarCalculatorSeries(filteredSeriesCandidates)
	}

	return nil
}

// Recursively iterate through the AST and compute the result.
//
// The existing ast.Walk() doesn't work here, because in ast.Walk(), visiting children is the last
// step of the function that visits parent, so the information in child nodes cannot be passed
// above in the iteration.
func (c *varCalculator) evaluateExpr(node ast.Node) (*varCalculatorSeries, error) {
	// If a node is of type *ast.Ident, it is a leaf with a series value.
	// Otherwise, it might be *ast.ParenExpr or *ast.BinaryExpr, so we continue recursing it to
	// compute the series value for the subtree..
	computeChildSeries := func(node ast.Node) (*varCalculatorSeries, error) {
		if reflect.TypeOf(node).String() == "*ast.Ident" {
			return c.varInfoMap[node.(*ast.Ident).Name].chosenSeries, nil
		}
		return c.evaluateExpr(node)
	}

	// Compute new series value of the *ast.BinaryExpr.
	// Supported operations are: +, -, *, /.
	evaluateBinaryExpr := func(
		x, y *varCalculatorSeries, op token.Token) (*varCalculatorSeries, error) {
		res := &varCalculatorSeries{points: []*pb.PointStat{}}

		// Upper stream guarantees that x.points and y.points have same dates.
		seriesLength := len(x.points)

		for i := 0; i < seriesLength; i++ {
			xVal := x.points[i].GetValue()
			yVal := y.points[i].GetValue()
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
			res.points = append(res.points, &pb.PointStat{
				Date:  x.points[i].GetDate(),
				Value: val,
			})
		}

		return res, nil
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
// For example, for an expression "Count_Person - Count_Person_Female[m=USCensus]", it returns
// {Count_Person, Count_Person_Female[m=USCensus]}.
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

// TODO(spaceenter): Use a regex to validate varName.
//
// Parse var name, which contains a variable and a set of filters.
// For example: Person_Count[m=US_Census;p=P1Y].
func parseVarName(varName string) (*varInfo, error) {
	res := &varInfo{}

	if strings.Contains(varName, "[") { // With filters.
		if !strings.Contains(varName, "]") {
			return nil, fmt.Errorf("missing ]")
		}

		leftBracketIndex := strings.Index(varName, "[")

		res.statMetadata = &pb.StatMetadata{}
		filterString := varName[leftBracketIndex+1 : len(varName)-1]
		for _, filter := range strings.Split(filterString, ";") {
			filterVal := filter[2:]
			switch filter[0:1] {
			case "m":
				res.statMetadata.MeasurementMethod = filterVal
			case "p":
				res.statMetadata.ObservationPeriod = filterVal
			case "u":
				res.statMetadata.Unit = filterVal
			case "s":
				res.statMetadata.ScalingFactor = filterVal
			}
		}

		res.statVar = varName[0:leftBracketIndex]

	} else { // No filters.
		res.statVar = varName
	}

	return res, nil
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
	decodeForParseTokenMap = map[string]string{
		"_DC_SLASH_":             "dc/",
		"_DC_AGGREGATE_SLASH_":   "dcAggregate/",
		"_LEFT_SQUARE_BRACKET_":  "[",
		"_RIGHT_SQUARE_BRACKET_": "]",
		"_EQUAL_TO_":             "=",
		"_SEMICOLON_":            ";",
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
	for k, v := range decodeForParseTokenMap {
		res = strings.ReplaceAll(res, k, v)
	}
	return res
}

func toVarCalculatorSeries(sourceSeries *pb.SourceSeries) *varCalculatorSeries {
	series := &varCalculatorSeries{
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

// Find common strings among a list of string sets.
// For example, for input [{a,b,c},{a,c,d},{a,c,e}], it returns {a,c}.
func commonStringsAmongStringSets(
	list []map[string]struct{}) map[string]struct{} {
	uniqueStringSet := map[string]struct{}{}
	for _, set := range list {
		for str := range set {
			uniqueStringSet[str] = struct{}{}
		}
	}

	res := map[string]struct{}{}
	for str := range uniqueStringSet {
		isCommonStr := true
		for _, set := range list {
			if _, ok := set[str]; !ok {
				isCommonStr = false
				break
			}
		}
		if isCommonStr {
			res[str] = struct{}{}
		}
	}

	return res
}

// TODO(spaceenter): Implement better ranking algorithm than simple string comparisons.
//
// The input `seriesCandidates` all have the same dates.
func rankVarCalculatorSeries(seriesCandidates []*varCalculatorSeries) *varCalculatorSeries {
	statMetadataKey := func(statMetadata *pb.StatMetadata) string {
		return strings.Join([]string{
			statMetadata.GetMeasurementMethod(),
			statMetadata.GetObservationPeriod(),
			statMetadata.GetUnit(),
			statMetadata.GetScalingFactor()}, "-")
	}

	var res *varCalculatorSeries
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
