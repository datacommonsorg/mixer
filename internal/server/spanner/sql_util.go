// Copyright 2026 Google LLC
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

package spanner

import (
	"fmt"
	"strings"

	cloudSpanner "cloud.google.com/go/spanner"
)

// InterpolateSQL replaces params with values in SQL.
// This is primarily used for debugging and testing to see the final query.
func InterpolateSQL(stmt *cloudSpanner.Statement) string {
	// Apply the same unrolling logic that executeQuery uses, so tests reflect reality.
	UnrollParameters(stmt)

	sqlString := stmt.SQL

	// Sort keys by length descending to prevent replacing substrings (e.g. @param before @param_0)
	var keys []string
	for k := range stmt.Params {
		keys = append(keys, k)
	}
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if len(keys[i]) < len(keys[j]) {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	for _, key := range keys {
		value := stmt.Params[key]
		placeholder := "@" + key
		var formattedValue string

		switch v := value.(type) {
		case string:
			formattedValue = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
		case []string:
			// For UNNEST, represent the array as a comma-separated list
			// enclosed in parentheses or brackets for clarity.
			var quotedValues []string
			for _, s := range v {
				quotedValues = append(quotedValues, fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "''")))
			}
			formattedValue = "(" + strings.Join(quotedValues, ",") + ")"
			// Need to handle both UNNEST(@key) and @key
			sqlString = strings.ReplaceAll(sqlString, "IN UNNEST("+placeholder+")", "IN "+formattedValue)
			placeholder = "@" + key // Ensure we don't mess up UNNEST replacement
			formattedValue = "[" + strings.Join(quotedValues, ",") + "]"
		case []float64:
			var stringValues []string
			for _, f := range v {
				stringValues = append(stringValues, fmt.Sprintf("%v", f))
			}
			formattedValue = "[" + strings.Join(stringValues, ",") + "]"
		// ... add more cases for int64, float64, bool, etc.
		default:
			// Catch-all for other types
			formattedValue = fmt.Sprintf("%v", v)
		}
		sqlString = strings.ReplaceAll(sqlString, placeholder, formattedValue)
	}
	return sqlString
}

// UnrollArrayParamsMaxLength specifies the maximum length of array parameters to unroll into explicit IN clauses.
const UnrollArrayParamsMaxLength = 10

// UnrollParameters modifies the given Spanner statement to unroll array parameters
// of length <= UnrollArrayParamsMaxLength. It replaces "IN UNNEST(@param)" with "IN (@param_0, @param_1, ...)"
// (or "= @param_0" if length is 1) and populates the params map with the individual values.
// The original array parameter is kept in case it is referenced elsewhere in the query (e.g. UNNEST(@param) without IN).
func UnrollParameters(stmt *cloudSpanner.Statement) {
	if stmt.Params == nil {
		return
	}

	newParams := make(map[string]interface{})
	for k, v := range stmt.Params {
		newParams[k] = v
	}

	for key, value := range stmt.Params {
		switch v := value.(type) {
		case []string:
			l := len(v)
			if l > 0 && l <= UnrollArrayParamsMaxLength {
				placeholder := "IN UNNEST(@" + key + ")"
				if strings.Contains(stmt.SQL, placeholder) {
					if l == 1 {
						newName := fmt.Sprintf("%s_0", key)
						newParams[newName] = v[0]
						stmt.SQL = strings.ReplaceAll(stmt.SQL, placeholder, "= @"+newName)
					} else {
						var paramNames []string
						for i, val := range v {
							newName := fmt.Sprintf("%s_%d", key, i)
							paramNames = append(paramNames, "@"+newName)
							newParams[newName] = val
						}
						replacement := "IN (" + strings.Join(paramNames, ",") + ")"
						stmt.SQL = strings.ReplaceAll(stmt.SQL, placeholder, replacement)
					}
				}
			}
		}
	}
	stmt.Params = newParams
}
