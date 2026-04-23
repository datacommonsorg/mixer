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
	"sort"
	"strings"

	"cloud.google.com/go/spanner"
	pb_int "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	"github.com/datacommonsorg/mixer/internal/server/sdmx"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
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

	return stmt
}

// GetNormalizedStatVarsByEntityQuery returns a query to fetch distinct variable and entity pairs for the normalized schema.
func GetNormalizedStatVarsByEntityQuery(variables []string, entities []string) (*spanner.Statement, error) {
	if len(variables) == 0 && len(entities) == 0 {
		return nil, fmt.Errorf("GetNormalizedStatVarsByEntityQuery must be called with at least one variable or entity")
	}
	sql := statementsNormalized.getStatVarsByEntity
	params := map[string]interface{}{}

	var filters []string
	if len(variables) > 0 {
		filter, val := getParamStatement("variables", variables)
		params["variables"] = val
		filters = append(filters, fmt.Sprintf("ts.variable_measured %s", filter))
	}
	if len(entities) > 0 {
		filter, val := getParamStatement("entities", entities)
		params["entities"] = val
		filters = append(filters, fmt.Sprintf("a.value %s", filter))
	}

	if len(filters) > 0 {
		sql += "\n\t\tWHERE " + strings.Join(filters, " AND ")
	}

	return &spanner.Statement{
		SQL:    sql,
		Params: params,
	}, nil
}

// GetNormalizedObservationsContainedInPlaceQuery returns a query to fetch observations for entities contained in a place.
func GetNormalizedObservationsContainedInPlaceQuery(variables []string, containedInPlace *v2.ContainedInPlace) *spanner.Statement {
	stmt := &spanner.Statement{
		SQL: statementsNormalized.getObsByContainedInPlace,
		Params: map[string]interface{}{
			"ancestor":       containedInPlace.Ancestor,
			"childPlaceType": containedInPlace.ChildPlaceType,
		},
	}

	if len(variables) > 0 {
		filter, val := getParamStatement("variables", variables)
		stmt.Params["variables"] = val
		stmt.SQL += "\n\t\tWHERE ts.variable_measured " + filter
	}

	return stmt
}

// GetSdmxObservationsQuery returns a query to fetch observations based on SDMX constraints.
func GetSdmxObservationsQuery(req *pb_int.SdmxDataQuery) *spanner.Statement {
	stmt := &spanner.Statement{
		SQL:    statementsNormalized.getSdmxObs, // Base query
		Params: map[string]interface{}{},
	}

	cMap := req.GetConstraints()
	if cMap == nil {
		return stmt
	}

	var subqueryFilters []string
	if startReq, ok := cMap[sdmx.ParamStartPeriod]; ok && len(startReq.GetValues()) > 0 {
		stmt.Params[sdmx.ParamStartPeriod] = startReq.GetValues()[0]
		subqueryFilters = append(subqueryFilters, "date >= @startPeriod")
	}
	if endReq, ok := cMap[sdmx.ParamEndPeriod]; ok && len(endReq.GetValues()) > 0 {
		stmt.Params[sdmx.ParamEndPeriod] = endReq.GetValues()[0]
		subqueryFilters = append(subqueryFilters, "date <= @endPeriod")
	}

	if len(subqueryFilters) > 0 {
		stmt.SQL = strings.Replace(stmt.SQL, "WHERE id = ts.id", "WHERE id = ts.id AND "+strings.Join(subqueryFilters, " AND "), 1)
	}

	var filters []string

	// Extract variableMeasured if present
	if varMeasured, ok := cMap[sdmx.DimVariableMeasured]; ok && len(varMeasured.GetValues()) > 0 {
		stmt.Params[sdmx.DimVariableMeasured] = varMeasured.GetValues()[0]
		filters = append(filters, "ts.variable_measured = @variableMeasured")
	}

	// Handle other constraints
	var props []string
	for prop := range cMap {
		if prop == sdmx.DimVariableMeasured || prop == sdmx.ParamStartPeriod || prop == sdmx.ParamEndPeriod {
			continue // Already handled
		}
		props = append(props, prop)
	}
	sort.Strings(props)

	paramIdx := 0
	for _, prop := range props {
		vals := cMap[prop].GetValues()
		if len(vals) == 0 {
			continue
		}

		paramName := fmt.Sprintf("param_%d", paramIdx)
		propParam := fmt.Sprintf("prop_%d", paramIdx)
		stmt.Params[propParam] = prop

		if len(vals) > 1 {
			stmt.Params[paramName] = vals
			filters = append(filters, fmt.Sprintf("ts.id IN (SELECT id FROM TimeSeriesAttribute@{FORCE_INDEX=TimeSeriesAttributePropertyValue} WHERE property = @%s AND value IN UNNEST(@%s))", propParam, paramName))
		} else {
			stmt.Params[paramName] = vals[0]
			filters = append(filters, fmt.Sprintf("ts.id IN (SELECT id FROM TimeSeriesAttribute@{FORCE_INDEX=TimeSeriesAttributePropertyValue} WHERE property = @%s AND value = @%s)", propParam, paramName))
		}
		paramIdx++
	}

	if len(filters) > 0 {
		stmt.SQL += "\n\t\tWHERE " + strings.Join(filters, " AND ")
	}

	return stmt
}
