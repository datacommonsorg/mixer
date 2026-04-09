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

	"cloud.google.com/go/spanner"
)

// GetNormalizedObservationsQuery returns a query to fetch observations based on variables and entities for the normalized schema.
func GetNormalizedObservationsQuery(variables []string, entities []string) *spanner.Statement {
	stmt := &spanner.Statement{
		SQL:    statementsNormalized.getObs,
		Params: map[string]interface{}{},
	}

	filters := []string{}
	if len(variables) > 0 {
		variableFilter, variableVal := getParamStatement("variables", variables)
		stmt.Params["variables"] = variableVal
		filters = append(filters, fmt.Sprintf(statementsNormalized.selectVariableDcids, variableFilter))
	}
	if len(entities) > 0 {
		entityFilter, entityVal := getParamStatement("entities", entities)
		stmt.Params["entities"] = entityVal
		filters = append(filters, fmt.Sprintf(statementsNormalized.selectEntityDcids, entityFilter))
	}

	if len(filters) > 0 {
		stmt.SQL += "\n\t\tWHERE " + strings.Join(filters, " AND ")
	}

	stmt.SQL += "\n\t\tORDER BY svo.date ASC"

	return stmt
}

// GetTimeSeriesAttributesQuery returns a query to fetch attributes for a list of time series IDs.
func GetTimeSeriesAttributesQuery(ids []string) *spanner.Statement {
	return &spanner.Statement{
		SQL: statementsNormalized.getTimeSeriesAttributes,
		Params: map[string]interface{}{
			"ids": ids,
		},
	}
}
