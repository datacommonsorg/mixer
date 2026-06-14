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

// GetMultiEntityGroupPlaceExistenceQuery returns a query to check SVG/topic existence for places across entity slots.
func GetMultiEntityGroupPlaceExistenceQuery(variableGroups []string, entities []string, predicate string) *spanner.Statement {
	return &spanner.Statement{
		SQL: statementsMultiEntity.checkGroupPlaceExistence,
		Params: map[string]interface{}{
			"variableGroups": variableGroups,
			"entities":       entities,
			"predicate":      predicate,
		},
	}
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
		// TODO(rohitrkumar): Legacy api does not support it. remove?
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

func filterMultiEntityDescendentStatVarsQuery(constrainedPlaces []string, constrainedProvenance string, numEntitiesExistence int) *spanner.Statement {
	params := map[string]interface{}{}
	useEntitySlots := len(constrainedPlaces) > 0 || (constrainedProvenance == "" && numEntitiesExistence > 1)

	timeSeriesSource := fmt.Sprintf(statementsMultiEntity.selectDescendentStatVarsFromTimeSeries, timeSeriesTable)
	distinctExistenceKey := "ts.entity1"
	if useEntitySlots {
		timeSeriesSource = multiEntityDescendentStatVarsSlotsSQL(len(constrainedPlaces) > 0)
		distinctExistenceKey = "ts.entity"
		if len(constrainedPlaces) > 0 {
			params["places"] = constrainedPlaces
		}
	}

	var provenanceJoin string
	var provenanceFilters []string
	if constrainedProvenance != "" {
		provenanceJoin = statementsMultiEntity.joinDescendentStatVarsByProvenance
		provenanceFilters = append(provenanceFilters,
			statementsMultiEntity.filterDescendentStatVarsByProvenancePredicate,
			statementsMultiEntity.filterDescendentStatVarsByProvenanceObject,
		)
		params["predicate"] = getImportFilterPredicate(constrainedProvenance)
		params["provenance"] = constrainedProvenance
		distinctExistenceKey = "e1.subject_id"
	}

	var provenanceFilterSQL string
	if len(provenanceFilters) > 0 {
		provenanceFilterSQL = "\n\t\t\t\t\tWHERE " + strings.Join(provenanceFilters, "\n\t\t\t\t\t  AND ")
	}

	var existenceThreshold string
	if numEntitiesExistence > 1 {
		existenceThreshold = fmt.Sprintf(
			statementsMultiEntity.filterDescendentStatVarsByNumEntitiesExistence,
			distinctExistenceKey,
		)
		params["numEntitiesExistence"] = numEntitiesExistence
	}

	return &spanner.Statement{
		SQL: fmt.Sprintf(
			statementsMultiEntity.filterDescendentStatVarsByTimeSeries,
			timeSeriesSource,
			provenanceJoin,
			provenanceFilterSQL,
			existenceThreshold,
		),
		Params: params,
	}
}

func multiEntityDescendentStatVarsSlotsSQL(filterPlaces bool) string {
	entity1Filter := ""
	entity2Filter := statementsMultiEntity.filterEntity2Exists
	entity3Filter := statementsMultiEntity.filterEntity3Exists
	if filterPlaces {
		entity1Filter = statementsMultiEntity.filterEntity1ByPlaces
		entity2Filter = statementsMultiEntity.filterEntity2ByPlaces
		entity3Filter = statementsMultiEntity.filterEntity3ByPlaces
	}

	return fmt.Sprintf(
		statementsMultiEntity.selectDescendentStatVarsFromEntitySlots,
		timeSeriesTable,
		timeSeriesByEntity2Index,
		timeSeriesByEntity3Index,
		entity1Filter,
		entity2Filter,
		entity3Filter,
	)
}

// GetMultiEntityFilteredSVGChildrenQuery returns a query to get SVG children using multi-entity TimeSeries filters.
func GetMultiEntityFilteredSVGChildrenQuery(template string, node string, constrainedPlaces []string, constrainedProvenance string, numEntitiesExistence int, includeDefinitions bool) *spanner.Statement {
	subquery := filterMultiEntityDescendentStatVarsQuery(constrainedPlaces, constrainedProvenance, numEntitiesExistence)
	subquery.Params["node"] = node

	var baseStatement string
	switch template {
	case templateSV:
		if includeDefinitions {
			baseStatement = statements.getFilteredChildSVsWithDefinitions
		} else {
			baseStatement = statements.getFilteredChildSVs
		}
	case templateSVG:
		baseStatement = statements.getFilteredChildSVGs
	}

	return &spanner.Statement{
		SQL:    fmt.Sprintf(baseStatement, subquery.SQL),
		Params: subquery.Params,
	}
}

// GetMultiEntityFilteredTopicChildrenQuery returns a query to get Topic children using multi-entity TimeSeries filters.
func GetMultiEntityFilteredTopicChildrenQuery(nodes []string, constrainedPlaces []string, constrainedProvenance string, numEntitiesExistence int) *spanner.Statement {
	subquery := filterMultiEntityDescendentStatVarsQuery(constrainedPlaces, constrainedProvenance, numEntitiesExistence)

	nodeFilter, nodeVal := getParamStatement("node", nodes)
	subquery.Params["node"] = nodeVal

	return &spanner.Statement{
		SQL:    fmt.Sprintf(statements.getFilteredTopic, subquery.SQL, nodeFilter),
		Params: subquery.Params,
	}
}
