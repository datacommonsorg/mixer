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
	"regexp"
	"sort"
	"strings"
	"sync"

	cloudSpanner "cloud.google.com/go/spanner"
)

// notInPrefixRegex matches a leading NOT keyword within a regex match.
var notInPrefixRegex = regexp.MustCompile(`(?i)(?:^|\s)NOT\s+IN`)

// inUnnestRegexCache caches compiled regexes by param key to avoid
// recompiling on every query execution.
var inUnnestRegexCache sync.Map

// inUnnestRegex returns a regex that matches "IN UNNEST(@paramKey)" or
// "NOT IN UNNEST(@paramKey)" where IN is a standalone SQL keyword
// (preceded by whitespace or start-of-line, optionally preceded by NOT),
// preventing false matches with "JOIN UNNEST(@paramKey)" where JOIN ends with IN.
// Compiled regexes are cached since the set of param keys is small and reused.
func inUnnestRegex(paramKey string) *regexp.Regexp {
	if cached, ok := inUnnestRegexCache.Load(paramKey); ok {
		return cached.(*regexp.Regexp)
	}
	compiled := regexp.MustCompile(`(?i)(^|\s)(?:NOT\s+)?IN\s+UNNEST\(@` + regexp.QuoteMeta(paramKey) + `\)`)
	actual, _ := inUnnestRegexCache.LoadOrStore(paramKey, compiled)
	return actual.(*regexp.Regexp)
}

// extractLeadingSpace returns the leading whitespace character from a regex
// match, or empty string if the match starts at the beginning of the string.
func extractLeadingSpace(match string) string {
	if len(match) > 0 {
		switch match[0] {
		case ' ', '\t', '\n', '\v', '\f', '\r':
			return string(match[0])
		}
	}
	return ""
}

// hasNotPrefix returns true if the regex match includes a preceding NOT keyword.
func hasNotPrefix(match string) bool {
	return notInPrefixRegex.MatchString(match)
}

// quoteSQLString returns a SQL-escaped string literal.
func quoteSQLString(s string) string {
	return fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "''"))
}

// InterpolateSQL replaces params with values in SQL.
// This is primarily used for debugging and testing to see the final query.
func InterpolateSQL(stmt *cloudSpanner.Statement) string {
	// Create a copy of the statement to avoid mutating the caller's statement.
	stmtCopy := &cloudSpanner.Statement{
		SQL:    stmt.SQL,
		Params: make(map[string]interface{}),
	}
	for k, v := range stmt.Params {
		stmtCopy.Params[k] = v
	}
	// Apply the same unrolling logic that executeQuery uses, so tests reflect reality.
	UnrollParameters(stmtCopy)

	sqlString := stmtCopy.SQL

	// Sort keys by length descending to prevent replacing substrings
	// (e.g. @param before @param_0).
	var keys []string
	for k := range stmtCopy.Params {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return len(keys[i]) > len(keys[j])
	})

	for _, key := range keys {
		value := stmtCopy.Params[key]
		placeholder := "@" + key
		var formattedValue string

		switch v := value.(type) {
		case string:
			formattedValue = quoteSQLString(v)
		case []string:
			// For IN UNNEST(@key), represent the array as a parenthesized list.
			var quotedValues []string
			for _, s := range v {
				quotedValues = append(quotedValues, quoteSQLString(s))
			}
			parenthesized := "(" + strings.Join(quotedValues, ",") + ")"
			// Replace "IN UNNEST(@key)" (or "NOT IN UNNEST(@key)") using word-boundary matching.
			sqlString = inUnnestRegex(key).ReplaceAllStringFunc(sqlString, func(match string) string {
				prefix := "IN "
				if hasNotPrefix(match) {
					prefix = "NOT IN "
				}
				return extractLeadingSpace(match) + prefix + parenthesized
			})
			// For remaining @key references (e.g. FROM UNNEST(@key)), use array literal.
			formattedValue = "[" + strings.Join(quotedValues, ",") + "]"
		case []float64:
			var stringValues []string
			for _, f := range v {
				stringValues = append(stringValues, fmt.Sprintf("%v", f))
			}
			formattedValue = "[" + strings.Join(stringValues, ",") + "]"
		default:
			formattedValue = fmt.Sprintf("%v", v)
		}
		sqlString = strings.ReplaceAll(sqlString, placeholder, formattedValue)
	}
	return sqlString
}

// UnrollArrayParamsMaxLength specifies the maximum length of array parameters to unroll into explicit IN clauses.
const UnrollArrayParamsMaxLength = 10

// UnrollParameters modifies the given Spanner statement to unroll array parameters
// of length <= UnrollArrayParamsMaxLength. It replaces "IN UNNEST(@param)" with
// "IN (@param_0, @param_1, ...)" (or "= @param_0" if length is 1) and populates
// the params map with the individual values. The original array parameter is kept
// in case it is referenced elsewhere in the query (e.g. UNNEST(@param) without IN).
func UnrollParameters(stmt *cloudSpanner.Statement) {
	if stmt.Params == nil {
		return
	}

	newParams := make(map[string]interface{})
	for k, v := range stmt.Params {
		newParams[k] = v
	}

	for key, value := range stmt.Params {
		v, ok := value.([]string)
		if !ok {
			continue
		}
		l := len(v)
		if l == 0 || l > UnrollArrayParamsMaxLength {
			continue
		}
		// Use regex to match "IN UNNEST(@key)" (or "NOT IN UNNEST(@key)") where
		// IN is a standalone keyword. This prevents false matches like
		// "JOIN UNNEST(@key)" where JOIN ends with IN.
		regex := inUnnestRegex(key)
		stmt.SQL = regex.ReplaceAllStringFunc(stmt.SQL, func(match string) string {
			leadingSpace := extractLeadingSpace(match)
			notPrefix := hasNotPrefix(match)
			if l == 1 {
				newName := key + "_0"
				newParams[newName] = v[0]
				eqOp := "="
				if notPrefix {
					eqOp = "!="
				}
				return leadingSpace + eqOp + " @" + newName
			}
			var paramNames []string
			for i, val := range v {
				newName := fmt.Sprintf("%s_%d", key, i)
				paramNames = append(paramNames, "@"+newName)
				newParams[newName] = val
			}
			inPrefix := "IN ("
			if notPrefix {
				inPrefix = "NOT IN ("
			}
			return leadingSpace + inPrefix + strings.Join(paramNames, ",") + ")"
		})
	}
	stmt.Params = newParams
}
