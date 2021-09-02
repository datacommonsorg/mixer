// Copyright 2021 Google LLC
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

package parser

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"

	"github.com/datacommonsorg/mixer/internal/base"
)

// Parse parses a complex value string into node.
//
// [!!]This has minimal validation check, assuming the input has been checked
// by the import tool.
//
// 1. a Quantity/QuantityRange value, coded as one of:
//  [<unit> <val>]
//  [<unit> <startval> <endval>]
//  [<unit> - <endval>]
//  [<unit> <startval> -]
//
// 2. a GeoCoordinates type, coded as one of:
//  [LatLong <lat_value> <long_value>]
//  [<lat_value> <long_value> LatLong]
func ParseComplexValue(val string) string {
	trimmed := strings.Trim(val, "[]")
	fields := strings.Fields(trimmed)
	isRange := len(fields) == 3
	startIdx, endIdx, valueIdx, unitIdx := -1, -1, -1, -1
	if fields[0][0] == '-' || unicode.IsDigit([]rune(fields[0])[0]) {
		// First field is value
		if isRange {
			unitIdx = 2
			startIdx = 0
			endIdx = 1
		} else {
			unitIdx = 1
			valueIdx = 0
		}
	} else {
		// First field is unit
		if isRange {
			unitIdx = 0
			startIdx = 1
			endIdx = 2
		} else {
			unitIdx = 0
			valueIdx = 1
		}
	}
	// Get unit.
	var unit string
	colonIndex := strings.IndexRune(fields[unitIdx], base.ReferenceDelimiter)
	if colonIndex != -1 {
		unit = fields[unitIdx][colonIndex+1:]
	} else {
		unit = fields[unitIdx]
	}

	// Compute DCID.
	var dcid string
	if len(fields) == 2 {
		dcid = unit + fields[valueIdx]
	} else {
		// len(fields) == 3
		if strings.ToLower(unit) == "latlong" {
			dcid, _ = parseLatLng(fields[startIdx], fields[endIdx])
		} else {
			dcid, _ = parseQuantityRange(fields[startIdx], fields[endIdx], unit)
		}
	}
	return dcid
}

func parseLatLng(latStr, lngStr string) (string, string) {
	if strings.HasSuffix(strings.ToUpper(latStr), "N") {
		latStr = latStr[:len(latStr)-1]
	} else if strings.HasSuffix(strings.ToUpper(latStr), "S") {
		latStr = "-" + latStr[:len(latStr)-1]
	}
	// Ignore error, assuming the input has been validated.
	lat, _ := strconv.ParseFloat(latStr, 64)

	if strings.HasSuffix(strings.ToUpper(lngStr), "E") {
		lngStr = lngStr[:len(lngStr)-1]
	} else if strings.HasSuffix(strings.ToUpper(lngStr), "W") {
		lngStr = "-" + lngStr[:len(lngStr)-1]
	}
	// Ignore error, assuming the input has been validated.
	lng, _ := strconv.ParseFloat(lngStr, 64)

	// E5 (1/100000th of a degree) or 1 meter is the maximum resolution we
	// support.
	lat_e5 := math.Round(1e5 * lat)
	lng_e5 := math.Round(1e5 * lng)
	latStr = fmt.Sprintf("%.5f", (lat_e5 / 1e5))
	lngStr = fmt.Sprintf("%.5f", (lng_e5 / 1e5))

	dcid := fmt.Sprintf("latLong/%.0f_%.0f", lat_e5, lng_e5)
	name := latStr + "," + lngStr
	return dcid, name
}

func parseQuantityRange(startVal, endVal, unit string) (string, string) {
	// Do not check validity, assuming input has been checked by import tool.
	var dcid, name string
	if startVal == "-" {
		dcid = unit + "Upto" + endVal
		name = unit + " UpTo " + endVal
	} else if endVal == "-" {
		dcid = unit + startVal + "Onwards"
		name = unit + " " + startVal + " Onwards"
	} else {
		dcid = unit + startVal + "To" + endVal
		name = unit + " " + startVal + " To " + endVal
	}
	return dcid, name
}
