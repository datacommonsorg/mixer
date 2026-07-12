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

	"cloud.google.com/go/spanner"
	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/server/v2/shared"
)

type multiEntityQueryBuilder struct {
	statements *MultiEntityStatements
}

// NewMultiEntityQueryBuilder builds a query builder using table-config-specific SQL templates.
func NewMultiEntityQueryBuilder(cfg TableConfig) (*multiEntityQueryBuilder, error) {
	stmts, err := NewMultiEntityStatements(cfg)
	if err != nil {
		return nil, err
	}
	return &multiEntityQueryBuilder{statements: stmts}, nil
}

// GetObservationsQuery builds the observation lookup query with optional date filter.
func (b *multiEntityQueryBuilder) GetObservationsQuery(variables []string, entities []string, date string) (*spanner.Statement, error) {
	stmts := b.statements
	if len(entities) == 0 {
		return nil, fmt.Errorf("GetObservationsQuery: entities must be specified")
	}

	var sql string
	params := map[string]interface{}{}

	if len(variables) > 0 {
		switch strings.ToUpper(date) {
		case "":
			sql = stmts.getObsBoth
		case shared.LATEST:
			sql = stmts.getObsBothLatest
		default:
			sql = stmts.getObsBothWithDate
			params["date"] = date
		}
		params["variables"] = variables
		params["entities"] = entities
	} else {
		switch strings.ToUpper(date) {
		case "":
			sql = stmts.getObsEntitiesOnly
		case shared.LATEST:
			sql = stmts.getObsEntitiesOnlyLatest
		default:
			sql = stmts.getObsEntitiesOnlyWithDate
			params["date"] = date
		}
		params["entities"] = entities
	}

	return &spanner.Statement{
		SQL:    sql,
		Params: params,
	}, nil
}

// GetStatVarsByEntityQuery builds the variable existence query across entity slots.
func (b *multiEntityQueryBuilder) GetStatVarsByEntityQuery(variables []string, entities []string) (*spanner.Statement, error) {
	stmts := b.statements
	if len(variables) == 0 && len(entities) == 0 {
		return nil, fmt.Errorf("GetStatVarsByEntityQuery: must be called with at least one variable or entity")
	}

	var sql string
	params := map[string]interface{}{}

	switch {
	case len(variables) > 0 && len(entities) > 0:
		sql = stmts.getStatVarsByEntityBoth
		params["variables"] = variables
		params["entities"] = entities
	case len(variables) > 0:
		sql = stmts.getStatVarsByEntityVarsOnly
		params["variables"] = variables
	default:
		sql = stmts.getStatVarsByEntityEntitiesOnly
		params["entities"] = entities
	}

	return &spanner.Statement{
		SQL:    sql,
		Params: params,
	}, nil
}

// GetGroupPlaceExistenceQuery returns a query to check SVG/topic existence for places across entity slots.
func (b *multiEntityQueryBuilder) GetGroupPlaceExistenceQuery(variableGroups []string, entities []string, predicate string) (*spanner.Statement, error) {
	stmts := b.statements
	return &spanner.Statement{
		SQL: stmts.checkGroupPlaceExistence,
		Params: map[string]interface{}{
			"variableGroups": variableGroups,
			"entities":       entities,
			"predicate":      predicate,
		},
	}, nil
}

// GetObservationsContainedInPlaceQuery builds the observation containment lookup query with optional date filter.
func (b *multiEntityQueryBuilder) GetObservationsContainedInPlaceQuery(variables []string, containedInPlace *v2.ContainedInPlace, date string) (*spanner.Statement, error) {
	stmts := b.statements
	if len(variables) == 0 {
		return nil, fmt.Errorf("GetObservationsContainedInPlaceQuery: variables must be specified")
	}
	if containedInPlace == nil {
		return nil, fmt.Errorf("GetObservationsContainedInPlaceQuery: containedInPlace must be specified")
	}

	params := map[string]interface{}{
		"ancestor":       containedInPlace.Ancestor,
		"childPlaceType": containedInPlace.ChildPlaceType,
		"variables":      variables,
	}

	var sql string
	switch strings.ToUpper(date) {
	case "":
		sql = stmts.getObsByContainedInPlaceBoth
	case shared.LATEST:
		sql = stmts.getObsByContainedInPlaceBothLatest
	default:
		sql = stmts.getObsByContainedInPlaceBothWithDate
		params["date"] = date
	}

	return &spanner.Statement{
		SQL:    sql,
		Params: params,
	}, nil
}

// GetStatVarGroupNodeQuery returns a query to get StatVarGroupNode info from the multi-entity schema.
func (b *multiEntityQueryBuilder) GetStatVarGroupNodeQuery(nodes []string, includeDefinitions bool) (*spanner.Statement, error) {
	stmts := b.statements
	nodeFilter, nodeVal := getParamStatement("nodes", nodes)

	selfFilter := "SELECT\n" +
		"\t\t\t\t@nodes AS child_svg,\n" +
		"\t\t\t\t@nodes AS svg"
	if len(nodes) > 1 {
		selfFilter = statements.attachSVGs
	}

	sqlTemplate := stmts.getStatVarGroupNode
	if includeDefinitions {
		sqlTemplate = stmts.getStatVarGroupNodeWithDefinitions
	}

	return &spanner.Statement{
		SQL: fmt.Sprintf(sqlTemplate, nodeFilter, selfFilter),
		Params: map[string]interface{}{
			"nodes": nodeVal,
		},
	}, nil
}

func filterMultiEntityDescendentStatVarsQuery(constrainedPlaces []string, constrainedProvenance string, numEntitiesExistence int, stmts *MultiEntityStatements) *spanner.Statement {
	params := map[string]interface{}{}
	useEntitySlots := len(constrainedPlaces) > 0 || (constrainedProvenance == "" && numEntitiesExistence > 1)

	timeSeriesSource := stmts.selectDescendentStatVarsFromTimeSeries
	distinctExistenceKey := "ts.entity1"
	if useEntitySlots {
		timeSeriesSource = multiEntityDescendentStatVarsSlotsSQL(len(constrainedPlaces) > 0, stmts)
		distinctExistenceKey = "ts.entity"
		if len(constrainedPlaces) > 0 {
			params["places"] = constrainedPlaces
		}
	}

	var provenanceJoin string
	var provenanceFilters []string
	if constrainedProvenance != "" {
		provenanceJoin = stmts.joinDescendentStatVarsByProvenance
		provenanceFilters = append(provenanceFilters,
			stmts.filterDescendentStatVarsByProvenancePredicate,
			stmts.filterDescendentStatVarsByProvenanceObject,
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
			stmts.filterDescendentStatVarsByNumEntitiesExistence,
			distinctExistenceKey,
		)
		params["numEntitiesExistence"] = numEntitiesExistence
	}

	return &spanner.Statement{
		SQL: fmt.Sprintf(
			stmts.filterDescendentStatVarsByTimeSeries,
			timeSeriesSource,
			provenanceJoin,
			provenanceFilterSQL,
			existenceThreshold,
		),
		Params: params,
	}
}

func multiEntityDescendentStatVarsSlotsSQL(filterPlaces bool, stmts *MultiEntityStatements) string {
	entity1Filter := ""
	entity2Filter := stmts.filterEntity2Exists
	entity3Filter := stmts.filterEntity3Exists
	if filterPlaces {
		entity1Filter = stmts.filterEntity1ByPlaces
		entity2Filter = stmts.filterEntity2ByPlaces
		entity3Filter = stmts.filterEntity3ByPlaces
	}

	return fmt.Sprintf(
		stmts.selectDescendentStatVarsFromEntitySlots,
		entity1Filter,
		entity2Filter,
		entity3Filter,
	)
}

// GetFilteredSVGChildrenQuery returns a query to get SVG children using multi-entity TimeSeries filters.
func (b *multiEntityQueryBuilder) GetFilteredSVGChildrenQuery(template string, node string, constrainedPlaces []string, constrainedProvenance string, numEntitiesExistence int, includeDefinitions bool) (*spanner.Statement, error) {
	stmts := b.statements
	subquery := filterMultiEntityDescendentStatVarsQuery(constrainedPlaces, constrainedProvenance, numEntitiesExistence, stmts)
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
	}, nil
}

// GetFilteredTopicChildrenQuery returns a query to get Topic children using multi-entity TimeSeries filters.
func (b *multiEntityQueryBuilder) GetFilteredTopicChildrenQuery(nodes []string, constrainedPlaces []string, constrainedProvenance string, numEntitiesExistence int) (*spanner.Statement, error) {
	stmts := b.statements
	subquery := filterMultiEntityDescendentStatVarsQuery(constrainedPlaces, constrainedProvenance, numEntitiesExistence, stmts)

	nodeFilter, nodeVal := getParamStatement("node", nodes)
	subquery.Params["node"] = nodeVal

	return &spanner.Statement{
		SQL:    fmt.Sprintf(statements.getFilteredTopic, subquery.SQL, nodeFilter),
		Params: subquery.Params,
	}, nil
}

var constraintKeyRegex = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// GetSdmxObservationsQuery builds the Spanner statement for SDMX observation lookup.
func (b *multiEntityQueryBuilder) GetSdmxObservationsQuery(
	constraints map[string]*sdmxpb.ConstraintList,
	entitySlotsByStatVar map[string]map[string]string,
) (*spanner.Statement, error) {
	compiled, err := compileSdmxConstraints(constraints, entitySlotsByStatVar)
	if err != nil {
		return nil, fmt.Errorf("GetSdmxObservationsQuery: %w", err)
	}

	return &spanner.Statement{
		SQL:    b.statements.getSdmxObs + compiled.where,
		Params: compiled.params,
	}, nil
}

type compiledSdmxConstraints struct {
	where      string
	params     map[string]interface{}
	statVarIDs []string
}

func compileSdmxConstraints(
	constraints map[string]*sdmxpb.ConstraintList,
	entitySlotsByStatVar map[string]map[string]string,
) (*compiledSdmxConstraints, error) {
	if constraints == nil {
		return nil, fmt.Errorf("request constraints cannot be nil")
	}
	for componentID, list := range constraints {
		if !constraintKeyRegex.MatchString(componentID) {
			return nil, fmt.Errorf("invalid constraint key %q", componentID)
		}
		if list == nil || len(list.GetValues()) == 0 {
			return nil, fmt.Errorf("constraint %q must have at least one value", componentID)
		}
		for _, value := range list.GetValues() {
			if strings.TrimSpace(value) == "" {
				return nil, fmt.Errorf("constraint %q contains an empty value", componentID)
			}
		}
	}

	variableMeasured, ok := constraints["variableMeasured"]
	if !ok || len(variableMeasured.GetValues()) == 0 {
		return nil, fmt.Errorf("variableMeasured must be specified")
	}
	statVarIDs := sortedUniqueStrings(variableMeasured.GetValues())

	componentIDs := []string{}
	for componentID := range constraints {
		if componentID != "variableMeasured" {
			componentIDs = append(componentIDs, componentID)
		}
	}
	sort.Strings(componentIDs)

	params := map[string]interface{}{}
	for _, componentID := range componentIDs {
		params[componentID] = constraints[componentID].GetValues()
	}

	statVarBranches := make([]string, 0, len(statVarIDs))
	for _, statVarID := range statVarIDs {
		clauses := []string{fmt.Sprintf("t.variable_measured = %q", statVarID)}
		entitySlots := entitySlotsByStatVar[statVarID]
		for _, componentID := range componentIDs {
			spannerColumn, ok := sdmxDataFilterColumn(componentID, entitySlots)
			if !ok {
				return nil, fmt.Errorf("unsupported constraint key %q", componentID)
			}
			clauses = append(clauses, sdmxAllowedValuesClause(spannerColumn, componentID))
		}
		statVarBranches = append(statVarBranches, "("+strings.Join(clauses, " AND ")+")")
	}

	return &compiledSdmxConstraints{
		where:      strings.Join(statVarBranches, " OR "),
		params:     params,
		statVarIDs: statVarIDs,
	}, nil
}

func sdmxAllowedValuesClause(spannerColumn string, parameter string) string {
	return fmt.Sprintf("t.%s IN UNNEST(@%s)", spannerColumn, parameter)
}

// GetSdmxAvailabilityQuery builds the SDMX availability lookup.
func (b *multiEntityQueryBuilder) GetSdmxAvailabilityQuery(
	req *sdmxpb.SdmxAvailabilityQuery,
	entitySlotsByStatVar map[string]map[string]string,
) (*spanner.Statement, error) {
	if req == nil {
		return nil, fmt.Errorf("GetSdmxAvailabilityQuery: request cannot be nil")
	}
	compiled, err := compileSdmxConstraints(req.GetConstraints(), entitySlotsByStatVar)
	if err != nil {
		return nil, fmt.Errorf("GetSdmxAvailabilityQuery: %w", err)
	}
	valueExpression, err := sdmxAvailabilityValueExpression(req.GetComponentId(), compiled.statVarIDs, entitySlotsByStatVar)
	if err != nil {
		return nil, err
	}

	return &spanner.Statement{
		SQL:    fmt.Sprintf(b.statements.getSdmxAvailability, valueExpression, compiled.where),
		Params: compiled.params,
	}, nil
}

func sdmxAvailabilityValueExpression(
	componentID string,
	statVarIDs []string,
	entitySlotsByStatVar map[string]map[string]string,
) (string, error) {
	if componentID == "variableMeasured" {
		return "t.variable_measured", nil
	}

	spannerColumn := ""
	for _, statVarID := range statVarIDs {
		column, ok := sdmxDataFilterColumn(componentID, entitySlotsByStatVar[statVarID])
		if !ok {
			return "", fmt.Errorf("GetSdmxAvailabilityQuery: unsupported component %q for stat var %q", componentID, statVarID)
		}
		if spannerColumn == "" {
			spannerColumn = column
			continue
		}
		if column != spannerColumn {
			return "", fmt.Errorf("GetSdmxAvailabilityQuery: inconsistent column for component %q across stat vars", componentID)
		}
	}
	if spannerColumn == "" {
		return "", fmt.Errorf("GetSdmxAvailabilityQuery: unsupported component %q", componentID)
	}
	return "t." + spannerColumn, nil
}
