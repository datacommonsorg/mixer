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

// TODO(spaceenter): Use a regex to validate nodeName.
//
// Parse nodeName, which contains a variable and a set of filters.
// For example: Person_Count[mm=US_Census;p=P1Y].
func parseNode(nodeName string) (*nodeData, error) {
	res := &nodeData{}

	if strings.Contains(nodeName, "[") { // With filters.
		if !strings.Contains(nodeName, "]") {
			return nil, fmt.Errorf("missing ]")
		}

		leftBracketIndex := strings.Index(nodeName, "[")

		res.statMetadata = &pb.StatMetadata{}
		filterString := nodeName[leftBracketIndex+1 : len(nodeName)-1]
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

		res.statVar = nodeName[0:leftBracketIndex]

	} else { // No filters.
		res.statVar = nodeName
	}

	return res, nil
}
