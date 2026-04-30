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
	sqlString := stmt.SQL
	for key, value := range stmt.Params {
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
