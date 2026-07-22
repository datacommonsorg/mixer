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
	"maps"
	"regexp"
	"slices"
	"sort"
	"strings"

	"cloud.google.com/go/spanner"
	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/server/v2/shared"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type multiEntityQueryBuilder struct {
	statements  *MultiEntityStatements
	tableConfig TableConfig
	queryConfig QueryConfig
}

// NewMultiEntityQueryBuilder builds a query builder using table and query configuration.
func NewMultiEntityQueryBuilder(
	tableConfig TableConfig,
	queryConfig QueryConfig,
) (*multiEntityQueryBuilder, error) {
	stmts, err := NewMultiEntityStatements(tableConfig)
	if err != nil {
		return nil, err
	}
	return &multiEntityQueryBuilder{
		statements:  stmts,
		tableConfig: tableConfig,
		queryConfig: queryConfig,
	}, nil
}

// GetObservationsQuery builds the observation lookup query with optional date filter.
func (b *multiEntityQueryBuilder) GetObservationsQuery(variables []string, entities []string, date string) (*spanner.Statement, error) {
	stmts := b.statements
	if len(entities) == 0 {
		return nil, fmt.Errorf("GetObservationsQuery: entities must be specified")
	}
	uniqueVariables := sortedUniqueStrings(variables)
	uniqueEntities := sortedUniqueStrings(entities)

	var sql string
	params := map[string]interface{}{}

	if len(uniqueVariables) > 0 {
		switch strings.ToUpper(date) {
		case "":
			sql = stmts.getObsBoth
		case shared.LATEST:
			sql = stmts.getObsBothLatest
		default:
			sql = stmts.getObsBothWithDate
			params["date"] = date
		}
		params["variables"] = uniqueVariables
		params["entities"] = uniqueEntities
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
		params["entities"] = uniqueEntities
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
	uniqueVariables := sortedUniqueStrings(variables)
	uniqueEntities := sortedUniqueStrings(entities)

	var sql string
	params := map[string]interface{}{}

	switch {
	case len(uniqueVariables) > 0 && len(uniqueEntities) > 0:
		sql = stmts.getStatVarsByEntityBoth
		params["variables"] = uniqueVariables
		params["entities"] = uniqueEntities
	case len(uniqueVariables) > 0:
		sql = stmts.getStatVarsByEntityVarsOnly
		params["variables"] = uniqueVariables
	default:
		sql = stmts.getStatVarsByEntityEntitiesOnly
		params["entities"] = uniqueEntities
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
	uniqueVariables := sortedUniqueStrings(variables)

	params := map[string]interface{}{
		"ancestor":       containedInPlace.Ancestor,
		"childPlaceType": containedInPlace.ChildPlaceType,
		"variables":      uniqueVariables,
	}

	containedInPlaceStatements := stmts.getObsByContainedInPlaceTypeFirst
	if b.queryConfig.containedInPlaceAccessPath(containedInPlace.ChildPlaceType) == containedInPlaceAncestorFirst {
		containedInPlaceStatements = stmts.getObsByContainedInPlaceAncestorFirst
	}

	selectedStatements := containedInPlaceStatements.variableSeek
	minVariables := b.queryConfig.ContainedInPlaceEntityScanMinVariables
	if minVariables > 0 && len(uniqueVariables) >= minVariables {
		// The base-table plan performs one sparse seek per place-variable pair.
		// For broad variable lists, scanning each entity1 index range once and
		// applying variable_measured as a residual filter can be cheaper. This
		// threshold is a heuristic because the builder does not know how many
		// TimeSeries rows belong to each selected place.
		selectedStatements = containedInPlaceStatements.entityScan
	}

	var sql string
	switch strings.ToUpper(date) {
	case "":
		sql = selectedStatements.all
	case shared.LATEST:
		sql = selectedStatements.latest
	default:
		sql = selectedStatements.withDate
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

	selfFilter := statements.attachSVGs

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
	constraints map[string]*sdmxpb.SdmxComponentConstraint,
	observationPropertyToEntitySlot map[string]string,
	containedInPlaceToRemoteDCIDs map[datacommons.ContainedInPlaceConstraint][]string,
) (*spanner.Statement, error) {
	// TODO: Parse SDMX constraints in prepareSdmxObservationsQuery and pass the
	// classified time and containment selections into this builder instead of
	// revalidating the raw constraint map.
	timeSelection, err := datacommons.ClassifyTimePeriod(constraints)
	if err != nil {
		return nil, status.Errorf(status.Code(err), "GetSdmxObservationsQuery: %s", status.Convert(err).Message())
	}
	containedInPlaceConstraints, err := datacommons.ContainedInPlaceConstraints(constraints)
	if err != nil {
		return nil, status.Errorf(status.Code(err), "GetSdmxObservationsQuery: %s", status.Convert(err).Message())
	}
	seriesConstraints := maps.Clone(constraints)
	delete(seriesConstraints, datacommons.ComponentTimePeriod)
	compiled, err := compileSdmxConstraints(seriesConstraints, observationPropertyToEntitySlot)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "GetSdmxObservationsQuery: %s", status.Convert(err).Message())
	}
	if len(containedInPlaceConstraints) > 0 {
		statement, err := b.getSdmxContainedInPlaceObservationsQuery(
			containedInPlaceConstraints,
			observationPropertyToEntitySlot,
			compiled,
			containedInPlaceToRemoteDCIDs,
			timeSelection,
		)
		if err != nil {
			return nil, status.Errorf(status.Code(err), "GetSdmxObservationsQuery: %s", status.Convert(err).Message())
		}
		return statement, nil
	}

	switch timeSelection.Mode {
	case datacommons.TimePeriodExplicit:
		params := maps.Clone(compiled.params)
		params["time_periods"] = timeSelection.Dates
		return &spanner.Statement{
			SQL:    fmt.Sprintf(b.statements.getSdmxObsWithDates, compiled.where),
			Params: params,
		}, nil
	case datacommons.TimePeriodLatest:
		return &spanner.Statement{
			SQL:    b.statements.getSdmxObsLatest + compiled.where,
			Params: compiled.params,
		}, nil
	default:
		return &spanner.Statement{
			SQL:    b.statements.getSdmxObs + compiled.where,
			Params: compiled.params,
		}, nil
	}
}

type compiledSdmxConstraints struct {
	where  string
	params map[string]interface{}
}

type resolvedSdmxDirectFilter struct {
	componentID   string
	spannerColumn string
	values        []string
}

func sdmxConstraintValues(constraint *sdmxpb.SdmxComponentConstraint) []string {
	predicates := constraint.GetPredicates()
	values := make([]string, 0, len(predicates))
	for _, predicate := range predicates {
		values = append(values, predicate.GetValue())
	}
	return values
}

func validateSdmxEntitySlotMapping(observationPropertyToEntitySlot map[string]string) error {
	entitySlotToObservationProperty := map[string]string{}
	for _, observationProperty := range slices.Sorted(maps.Keys(observationPropertyToEntitySlot)) {
		entitySlot := observationPropertyToEntitySlot[observationProperty]
		switch entitySlot {
		case "entity1", "entity2", "entity3":
		default:
			return status.Errorf(
				codes.InvalidArgument,
				"SDMX observation property %q maps to unsupported entity slot %q",
				observationProperty,
				entitySlot,
			)
		}
		if existingObservationProperty, ok := entitySlotToObservationProperty[entitySlot]; ok {
			return status.Errorf(
				codes.InvalidArgument,
				"SDMX observation properties %q and %q map to the same entity slot %q",
				existingObservationProperty,
				observationProperty,
				entitySlot,
			)
		}
		entitySlotToObservationProperty[entitySlot] = observationProperty
	}
	return nil
}

func compileSdmxConstraints(
	constraints map[string]*sdmxpb.SdmxComponentConstraint,
	observationPropertyToEntitySlot map[string]string,
) (*compiledSdmxConstraints, error) {
	if constraints == nil {
		return nil, status.Error(codes.InvalidArgument, "SDMX request constraints cannot be nil")
	}
	if err := validateSdmxEntitySlotMapping(observationPropertyToEntitySlot); err != nil {
		return nil, err
	}
	for componentID, constraint := range constraints {
		if !constraintKeyRegex.MatchString(componentID) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid SDMX component filter %q", componentID)
		}
		values := sdmxConstraintValues(constraint)
		if len(values) == 0 {
			if len(constraint.GetPropertyConstraints()) > 0 {
				continue
			}
			return nil, status.Errorf(codes.InvalidArgument, "SDMX component filter %q must have at least one value", componentID)
		}
		for _, value := range values {
			if strings.TrimSpace(value) == "" {
				return nil, status.Errorf(codes.InvalidArgument, "SDMX component filter %q contains an empty value", componentID)
			}
		}
	}

	variableMeasured, ok := constraints[datacommons.ComponentVariableMeasured]
	variableMeasuredValues := sdmxConstraintValues(variableMeasured)
	if !ok || len(variableMeasuredValues) == 0 {
		return nil, status.Error(codes.InvalidArgument, "SDMX component filter variableMeasured must be specified")
	}
	statVarIDs := sortedUniqueStrings(variableMeasuredValues)

	componentIDs := []string{}
	for componentID, constraint := range constraints {
		if componentID != datacommons.ComponentVariableMeasured && len(constraint.GetPredicates()) > 0 {
			componentIDs = append(componentIDs, componentID)
		}
	}
	sort.Strings(componentIDs)

	filters := make([]resolvedSdmxDirectFilter, 0, len(componentIDs))
	for _, componentID := range componentIDs {
		spannerColumn, ok := sdmxDataFilterColumn(componentID, observationPropertyToEntitySlot)
		if !ok || spannerColumn == "" {
			return nil, status.Errorf(codes.InvalidArgument, "unsupported SDMX component filter %q", componentID)
		}
		filters = append(filters, resolvedSdmxDirectFilter{
			componentID:   componentID,
			spannerColumn: spannerColumn,
			values:        sdmxConstraintValues(constraints[componentID]),
		})
	}
	sort.Slice(filters, func(i, j int) bool {
		if filters[i].spannerColumn != filters[j].spannerColumn {
			return filters[i].spannerColumn < filters[j].spannerColumn
		}
		return filters[i].componentID < filters[j].componentID
	})
	params := map[string]interface{}{
		datacommons.ComponentVariableMeasured: statVarIDs,
	}
	clauses := []string{sdmxAllowedValuesClause("variable_measured", datacommons.ComponentVariableMeasured)}
	for _, filter := range filters {
		parameter := "filter_" + filter.spannerColumn
		params[parameter] = filter.values
		clauses = append(clauses, sdmxAllowedValuesClause(filter.spannerColumn, parameter))
	}

	return &compiledSdmxConstraints{
		where:  strings.Join(clauses, " AND "),
		params: params,
	}, nil
}

func sdmxAllowedValuesClause(spannerColumn string, parameter string) string {
	return fmt.Sprintf("t.%s IN UNNEST(@%s)", spannerColumn, parameter)
}

type resolvedSdmxContainedInPlace struct {
	entityColumn string
	relation     datacommons.ContainedInPlaceConstraint
	cteName      string
}

func (b *multiEntityQueryBuilder) getSdmxContainedInPlaceObservationsQuery(
	constraints map[string]datacommons.ContainedInPlaceConstraint,
	observationPropertyToEntitySlot map[string]string,
	compiled *compiledSdmxConstraints,
	containedInPlaceToRemoteDCIDs map[datacommons.ContainedInPlaceConstraint][]string,
	timeSelection datacommons.TimePeriodSelection,
) (*spanner.Statement, error) {
	resolved := make([]resolvedSdmxContainedInPlace, 0, len(constraints))
	for _, componentID := range slices.Sorted(maps.Keys(constraints)) {
		relation := constraints[componentID]
		entityColumn, ok := observationPropertyToEntitySlot[componentID]
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "unsupported SDMX property constraint component %q", componentID)
		}
		resolved = append(resolved, resolvedSdmxContainedInPlace{
			entityColumn: entityColumn,
			relation:     relation,
		})
	}
	sort.Slice(resolved, func(i, j int) bool {
		leftPriority := sdmxContainmentAnchorPriority(resolved[i].entityColumn)
		rightPriority := sdmxContainmentAnchorPriority(resolved[j].entityColumn)
		if leftPriority != rightPriority {
			return leftPriority < rightPriority
		}
		return resolved[i].entityColumn < resolved[j].entityColumn
	})

	relationToCTE := map[datacommons.ContainedInPlaceConstraint]string{}
	cteDefinitions := []string{}
	params := maps.Clone(compiled.params)
	containedRule, _ := datacommons.DataPropertyRule(datacommons.PropertyContainedInPlace)
	typeRule, _ := datacommons.DataPropertyRule(datacommons.PropertyTypeOf)
	// TODO: Apply QueryConfig.ContainedInPlaceAncestorFirstTypes to SDMX containment CTEs.
	for i := range resolved {
		key := resolved[i].relation
		cteName, ok := relationToCTE[key]
		if !ok {
			cteIndex := len(relationToCTE)
			cteName = fmt.Sprintf("contained_places_%d", cteIndex)
			relationToCTE[key] = cteName
			ancestorParam := fmt.Sprintf("containment_%d_ancestor", cteIndex)
			childPlaceTypeParam := fmt.Sprintf("containment_%d_child_place_type", cteIndex)
			params[ancestorParam] = key.Ancestor
			params[childPlaceTypeParam] = key.ChildPlaceType
			remoteDCIDs := containedInPlaceToRemoteDCIDs[key]
			if len(remoteDCIDs) > 0 {
				remotePlacesParam := fmt.Sprintf("containment_%d_remote_places", cteIndex)
				params[remotePlacesParam] = remoteDCIDs
				cteDefinitions = append(cteDefinitions, fmt.Sprintf(
					b.statements.sdmxContainedPlacesWithRemoteCTE,
					cteName,
					containedRule.GraphPredicate,
					ancestorParam,
					typeRule.GraphPredicate,
					childPlaceTypeParam,
					remotePlacesParam,
				))
			} else {
				cteDefinitions = append(cteDefinitions, fmt.Sprintf(
					b.statements.sdmxContainedPlacesCTE,
					cteName,
					containedRule.GraphPredicate,
					ancestorParam,
					typeRule.GraphPredicate,
					childPlaceTypeParam,
				))
			}
		}
		resolved[i].cteName = cteName
	}

	// The anchor is the containment-constrained entity used to drive the
	// contained_places-to-TimeSeries join. Its entity slot selects the access
	// path; remaining containment constraints are applied as semi-join filters.
	anchor := resolved[0]
	index := "_BASE_TABLE"
	// TODO: Move SDMX containment statement hints into the statement templates
	// after the time-period query variants land, so every variant shares the same
	// production and emulator hint policy.
	// Production containment queries favor total scan and join throughput.
	statementHints := []string{"SCAN_METHOD=COLUMNAR", "EXECUTION_METHOD=BATCH"}
	if b.tableConfig.spannerEmulatorCompatibility {
		// The emulator cannot validate null-filtered index behavior. Apply its
		// per-query bypass uniformly to this query family; production never
		// receives this hint.
		statementHints = []string{"spanner_emulator.disable_query_null_filtered_index_check=true"}
	}
	whereClauses := []string{}
	// An entity3 anchor also requires entity2 to be non-null, which may overlap
	// with a separate entity2 containment constraint.
	addedNotNullFilters := map[string]struct{}{}
	appendNotNullFilter := func(entityColumn string) {
		if entityColumn != "entity2" && entityColumn != "entity3" {
			return
		}
		if _, added := addedNotNullFilters[entityColumn]; added {
			return
		}
		addedNotNullFilters[entityColumn] = struct{}{}
		whereClauses = append(whereClauses, fmt.Sprintf("t.%s IS NOT NULL", entityColumn))
	}
	switch anchor.entityColumn {
	case "entity2":
		index = b.tableConfig.TimeSeriesByEntity2Index
		appendNotNullFilter("entity2")
	case "entity3":
		index = b.tableConfig.TimeSeriesByEntity3Index
		appendNotNullFilter("entity3")
		appendNotNullFilter("entity2")
	}
	// Make null rejection explicit for nullable entity filters instead of relying
	// on Spanner to infer it from IN when considering null-filtered indexes.
	for _, constraint := range resolved[1:] {
		appendNotNullFilter(constraint.entityColumn)
		whereClauses = append(whereClauses, fmt.Sprintf("t.%s IN (SELECT place_id FROM %s)", constraint.entityColumn, constraint.cteName))
	}
	where := ""
	if len(whereClauses) > 0 {
		where = "\n\t\t\tWHERE " + strings.Join(whereClauses, "\n\t\t\t\tAND ")
	}

	seriesCTE := fmt.Sprintf(
		b.statements.sdmxContainedSeriesCTE,
		anchor.cteName,
		index,
		anchor.entityColumn,
		compiled.where,
		where,
	)

	statementHint := fmt.Sprintf("@{%s}\n\t\t", strings.Join(statementHints, ", "))
	queryTemplate := b.statements.getSdmxContainedInPlace
	switch timeSelection.Mode {
	case datacommons.TimePeriodExplicit:
		queryTemplate = b.statements.getSdmxContainedInPlaceWithDates
		params["time_periods"] = timeSelection.Dates
	case datacommons.TimePeriodLatest:
		queryTemplate = b.statements.getSdmxContainedInPlaceLatest
	}
	sql := fmt.Sprintf(
		queryTemplate,
		statementHint,
		strings.Join(cteDefinitions, ",\n\t\t"),
		seriesCTE,
	)

	return &spanner.Statement{SQL: sql, Params: params}, nil
}

func sdmxContainmentAnchorPriority(entityColumn string) int {
	// Prefer entity1's base-table path. If entity1 is unavailable, prefer entity3
	// over entity2 because TimeSeriesByEntity3 also stores entity2, allowing an
	// entity2 filter before fetching base-table rows.
	switch entityColumn {
	case "entity1":
		return 0
	case "entity3":
		return 1
	default:
		return 2
	}
}

type sdmxAvailabilityDateJoinPlan int

const (
	sdmxAvailabilityDateJoinAutomatic sdmxAvailabilityDateJoinPlan = iota
	sdmxAvailabilityDateJoinMergeBaseTable
)

type sdmxAvailabilityDateJoinContext struct {
	seriesOrderedByFullKey bool
	broadSeriesScan        bool
}

// selectSdmxAvailabilityDateJoinPlan chooses the physical join strategy at the
// TimeSeries-to-Observation boundary. A merge join is safe only when both inputs
// can be kept in (variable_measured, entity1, extra_entities_id, facet_id)
// order. Direct queries can guarantee that by forcing both base tables. A
// secondary index, containment anchor, candidate-series CTE, or local/remote
// union must be treated as unordered until a production query plan proves that
// it preserves the complete key order. Future containment builders should
// construct this context after choosing their series access path and default to
// automatic planning. Do not run a COUNT query to make this choice; if reliable
// cardinality metadata becomes available, add it to this context instead.
func selectSdmxAvailabilityDateJoinPlan(
	joinContext sdmxAvailabilityDateJoinContext,
) sdmxAvailabilityDateJoinPlan {
	if joinContext.seriesOrderedByFullKey && joinContext.broadSeriesScan {
		return sdmxAvailabilityDateJoinMergeBaseTable
	}
	return sdmxAvailabilityDateJoinAutomatic
}

func isBroadSdmxAvailabilitySeriesScan(
	constraints map[string]*sdmxpb.SdmxComponentConstraint,
) bool {
	for componentID := range constraints {
		if componentID != datacommons.ComponentVariableMeasured && componentID != datacommons.ComponentTimePeriod {
			return false
		}
	}
	return true
}

// GetSdmxAvailabilityQuery builds the SDMX availability lookup.
func (b *multiEntityQueryBuilder) GetSdmxAvailabilityQuery(
	req *sdmxpb.SdmxAvailabilityQuery,
	observationPropertyToEntitySlot map[string]string,
) (*spanner.Statement, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "SDMX availability request cannot be nil")
	}
	if err := datacommons.ValidateAvailabilityConstraints(req.GetConstraints()); err != nil {
		return nil, status.Errorf(status.Code(err), "GetSdmxAvailabilityQuery: %s", status.Convert(err).Message())
	}
	timeSelection, err := datacommons.ClassifyTimePeriod(req.GetConstraints())
	if err != nil {
		return nil, status.Errorf(status.Code(err), "GetSdmxAvailabilityQuery: %s", status.Convert(err).Message())
	}
	seriesConstraints := maps.Clone(req.GetConstraints())
	delete(seriesConstraints, datacommons.ComponentTimePeriod)
	compiled, err := compileSdmxConstraints(seriesConstraints, observationPropertyToEntitySlot)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "GetSdmxAvailabilityQuery: %s", status.Convert(err).Message())
	}
	valueExpression, err := sdmxAvailabilityValueExpression(req.GetComponentId(), observationPropertyToEntitySlot)
	if err != nil {
		return nil, err
	}

	queryTemplate := b.statements.getSdmxAvailability
	params := compiled.params
	if timeSelection.Mode == datacommons.TimePeriodExplicit {
		queryTemplate = b.statements.getSdmxAvailabilityWithDates
		joinPlan := selectSdmxAvailabilityDateJoinPlan(sdmxAvailabilityDateJoinContext{
			// The merge variant forces both base tables, which share the full
			// time-series key prefix required by the merge join.
			seriesOrderedByFullKey: true,
			broadSeriesScan:        isBroadSdmxAvailabilitySeriesScan(req.GetConstraints()),
		})
		if joinPlan == sdmxAvailabilityDateJoinMergeBaseTable {
			queryTemplate = b.statements.getSdmxAvailabilityWithDatesMergeBaseTable
		}
		params = maps.Clone(compiled.params)
		params["time_periods"] = timeSelection.Dates
	}

	return &spanner.Statement{
		SQL:    fmt.Sprintf(queryTemplate, valueExpression, compiled.where),
		Params: params,
	}, nil
}

func sdmxAvailabilityValueExpression(
	componentID string,
	observationPropertyToEntitySlot map[string]string,
) (string, error) {
	if componentID == datacommons.ComponentVariableMeasured {
		return "t.variable_measured", nil
	}

	spannerColumn, ok := sdmxDataFilterColumn(componentID, observationPropertyToEntitySlot)
	if !ok || spannerColumn == "" {
		return "", status.Errorf(codes.InvalidArgument, "unsupported SDMX availability component %q", componentID)
	}
	return "t." + spannerColumn, nil
}
