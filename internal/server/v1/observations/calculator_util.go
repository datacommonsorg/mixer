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

// Calculator util.

// Package observations contain code for observations.
package observations

import (
	"fmt"
	"go/token"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
)

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

// TODO(spaceenter): Use a regex to validate varName.
//
// Parse var name, which contains a variable and a set of filters.
// For example: Person_Count[mm=US_Census;p=P1Y].
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
			filterType := filter[0:2]
			filterVal := filter[3:]
			switch filterType {
			case "mm":
				res.statMetadata.MeasurementMethod = filterVal
			case "op":
				res.statMetadata.ObservationPeriod = filterVal
			case "ut":
				res.statMetadata.Unit = filterVal
			case "sf":
				res.statMetadata.ScalingFactor = filterVal
			default:
				return nil, fmt.Errorf("unsupported filter type: %s", filterType)
			}
		}

		res.statVar = varName[0:leftBracketIndex]

	} else { // No filters.
		res.statVar = varName
	}

	return res, nil
}

// Compute new series value of the *ast.BinaryExpr.
// Supported operations are: +, -, *, /.
func evaluateBinaryExpr(
	x, y *calculatorSeries, op token.Token) (*calculatorSeries, error) {
	res := &calculatorSeries{points: []*pb.PointStat{}}

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
