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
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/server/v2/shared"
)

// GetMultiEntityObservationsQuery builds the observation lookup query with optional date filter.
func GetMultiEntityObservationsQuery(variables []string, entities []string, date string) (*spanner.Statement, error) {
	if len(entities) == 0 {
		return nil, fmt.Errorf("GetMultiEntityObservationsQuery: entities must be specified")
	}

	var sql string
	params := map[string]interface{}{}

	if len(variables) > 0 {
		switch strings.ToUpper(date) {
		case "":
			sql = statementsMultiEntity.getObsBoth
		case shared.LATEST:
			sql = statementsMultiEntity.getObsBothLatest
		default:
			sql = statementsMultiEntity.getObsBothWithDate
			params["date"] = date
		}
		params["variables"] = variables
		params["entities"] = entities
	} else {
		switch strings.ToUpper(date) {
		case "":
			sql = statementsMultiEntity.getObsEntitiesOnly
		case shared.LATEST:
			sql = statementsMultiEntity.getObsEntitiesOnlyLatest
		default:
			sql = statementsMultiEntity.getObsEntitiesOnlyWithDate
			params["date"] = date
		}
		params["entities"] = entities
	}

	return &spanner.Statement{
		SQL:    sql,
		Params: params,
	}, nil
}

// GetMultiEntityStatVarsByEntityQuery builds the variable existence query across entity slots.
func GetMultiEntityStatVarsByEntityQuery(variables []string, entities []string) (*spanner.Statement, error) {
	if len(variables) == 0 && len(entities) == 0 {
		return nil, fmt.Errorf("GetMultiEntityStatVarsByEntityQuery: must be called with at least one variable or entity")
	}

	var sql string
	params := map[string]interface{}{}

	switch {
	case len(variables) > 0 && len(entities) > 0:
		sql = statementsMultiEntity.getStatVarsByEntityBoth
		params["variables"] = variables
		params["entities"] = entities
	case len(variables) > 0:
		sql = statementsMultiEntity.getStatVarsByEntityVarsOnly
		params["variables"] = variables
	default:
		sql = statementsMultiEntity.getStatVarsByEntityEntitiesOnly
		params["entities"] = entities
	}

	return &spanner.Statement{
		SQL:    sql,
		Params: params,
	}, nil
}

// GetMultiEntityObservationsContainedInPlaceQuery builds the observation containment lookup query with optional date filter.
func GetMultiEntityObservationsContainedInPlaceQuery(variables []string, containedInPlace *v2.ContainedInPlace, date string) (*spanner.Statement, error) {
	if containedInPlace == nil {
		return nil, fmt.Errorf("GetMultiEntityObservationsContainedInPlaceQuery: containedInPlace must be specified")
	}

	var sql string
	params := map[string]interface{}{
		"ancestor":       containedInPlace.Ancestor,
		"childPlaceType": containedInPlace.ChildPlaceType,
	}

	if len(variables) > 0 {
		switch strings.ToUpper(date) {
		case "":
			sql = statementsMultiEntity.getObsByContainedInPlaceBoth
		case shared.LATEST:
			sql = statementsMultiEntity.getObsByContainedInPlaceBothLatest
		default:
			sql = statementsMultiEntity.getObsByContainedInPlaceBothWithDate
			params["date"] = date
		}
		params["variables"] = variables
	} else {
		switch strings.ToUpper(date) {
		case "":
			sql = statementsMultiEntity.getObsByContainedInPlaceEntitiesOnly
		case shared.LATEST:
			sql = statementsMultiEntity.getObsByContainedInPlaceEntitiesOnlyLatest
		default:
			sql = statementsMultiEntity.getObsByContainedInPlaceEntitiesOnlyWithDate
			params["date"] = date
		}
	}

	return &spanner.Statement{
		SQL:    sql,
		Params: params,
	}, nil
}

// GetMultiEntityStatVarGroupNodeQuery returns a query to get StatVarGroupNode info from the multi-entity schema.
func GetMultiEntityStatVarGroupNodeQuery(nodes []string, includeDefinitions bool) *spanner.Statement {
	nodeFilter, nodeVal := getParamStatement("nodes", nodes)

	selfFilter := "SELECT\n" +
		"\t\t\t\t@nodes AS child_svg,\n" +
		"\t\t\t\t@nodes AS svg"
	if len(nodes) > 1 {
		selfFilter = statements.attachSVGs
	}

	sqlTemplate := statementsMultiEntity.getStatVarGroupNode
	if includeDefinitions {
		sqlTemplate = statementsMultiEntity.getStatVarGroupNodeWithDefinitions
	}

	return &spanner.Statement{
		SQL: fmt.Sprintf(sqlTemplate, nodeFilter, selfFilter),
		Params: map[string]interface{}{
			"nodes": nodeVal,
		},
	}
}
