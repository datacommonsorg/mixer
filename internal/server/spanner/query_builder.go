// Copyright 2025 Google LLC
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

// Queries builder for SpannerClient.
package spanner

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"cloud.google.com/go/spanner"
	"github.com/datacommonsorg/mixer/internal/merger"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	v3 "github.com/datacommonsorg/mixer/internal/server/v3"
	"github.com/datacommonsorg/mixer/internal/translator/types"
)

const (
	// SQL query snippets.
	// WHERE keyword for SQL queries.
	sqlWhere = "WHERE"
	// Prefix for graph queries with any node selection.
	sqlReturn = "\n\t\tRETURN"
	// DISTINCT keyword for SQL queries.
	sqlDistinct = " DISTINCT "
	// DESC keyword for SQL queries.
	sqlDesc = "\n\t\tDESC"
	// ORDER BY clause for SQL queries.
	sqlOrderBy = "\n\t\tORDER BY "
	// LIMIT clause for SQL queries.
	sqlLimit = "\n\t\tLIMIT @limit"
	// AND keyword for SQL queries.
	sqlAnd = "AND"
)

const (
	// Prefix length of value to include in object value ids.
	objectValuePrefix = 16
	// Template for fetching child SVs
	templateSV = "SV"
	// Template for fetching child SVGs
	templateSVG = "SVG"
	// Template for fetching children of a Topic
	templateTopic = "Topic"
	// isPartOf predicate
	predicateIsPartOf = "isPartOf"
	// source predicate
	predicateSource = "source"
)

func GetCompletionTimestampQuery() *spanner.Statement {
	return &spanner.Statement{
		SQL: statements.getCompletionTimestamp,
	}
}

func GetNodePropsQuery(ids []string, out bool) *spanner.Statement {
	idFilter, idVal := getParamStatement("id", ids)
	params := map[string]interface{}{
		"id": idVal,
	}

	switch out {
	case true:
		return &spanner.Statement{
			SQL:    fmt.Sprintf(statements.getPropsBySubjectID, idFilter),
			Params: params,
		}
	default:
		return &spanner.Statement{
			SQL:    fmt.Sprintf(statements.getPropsByObjectID, idFilter),
			Params: params,
		}
	}
}

func GetNodeEdgesByIDQuery(ids []string, arc *v2.Arc, pageSize, offset int) *spanner.Statement {
	idFilter, idVal := getParamStatement("id", ids)
	params := map[string]interface{}{
		"id": idVal,
	}

	// Attach predicates.
	filterPredicate := ""
	if arc.SingleProp != "" && arc.SingleProp != v3.Wildcard && arc.Decorator != v3.Chain {
		filterPredicate = statements.filterPredicate
		params["predicate"] = arc.SingleProp
	} else if len(arc.BracketProps) > 0 {
		filterPredicate = statements.filterPredicates
		params["predicate"] = arc.BracketProps
	}

	// Generate filters.
	subqueries := []string{}
	if len(arc.Filter) > 0 {
		// Sort for determinism.
		props := make([]string, 0, len(arc.Filter))
		for prop := range arc.Filter {
			props = append(props, prop)
		}
		sort.Strings(props)

		i := 0
		for _, prop := range props {
			params["prop"+strconv.Itoa(i)] = prop
			objectFilter := ""
			filterVal := addObjectValues(arc.Filter[prop])
			if len(filterVal) > 0 {
				if len(filterVal) == 1 {
					objectFilter = fmt.Sprintf(statements.filterValue, i)
					params["val"+strconv.Itoa(i)] = filterVal[0]
				} else {
					objectFilter = fmt.Sprintf(statements.filterValues, i)
					params["val"+strconv.Itoa(i)] = filterVal
				}
			}
			subqueries = append(subqueries, fmt.Sprintf(statements.filterProperty, i, objectFilter))
			i += 1
		}
	}

	var subquery string
	switch arc.Out {
	case true:
		if arc.Decorator == v3.Chain {
			subquery = fmt.Sprintf(statements.getChainedEdgesBySubjectID, idFilter, maxHops)
			params["predicate"] = arc.SingleProp
			params["result_predicate"] = arc.SingleProp + arc.Decorator
		} else {
			subquery = fmt.Sprintf(statements.getEdgesBySubjectID, idFilter, filterPredicate)
		}
	case false:
		if arc.Decorator == v3.Chain {
			subquery = fmt.Sprintf(statements.getChainedEdgesByObjectID, idFilter, maxHops)
			params["predicate"] = arc.SingleProp
			params["result_predicate"] = arc.SingleProp + arc.Decorator
		} else {
			subquery = fmt.Sprintf(statements.getEdgesByObjectID, idFilter, filterPredicate)
		}
	}
	subqueries = append([]string{subquery}, subqueries...)

	// Generate prefix and return statement.
	var prefix, returnEdges string
	switch arc.Decorator {
	case v3.Chain:
		prefix = statements.graphPrefixAny
		returnEdges = statements.returnChainedEdges
	default:
		prefix = statements.graphPrefix
		if len(arc.Filter) > 0 {
			returnEdges += statements.returnFilterEdges
		} else {
			returnEdges = statements.returnEdges
		}
	}

	template := prefix + strings.Join(subqueries, ",\n\t\t") + returnEdges

	// Apply pagination.
	if offset > 0 {
		template += fmt.Sprintf(statements.applyOffset, offset)
	}
	// Request pageSize+1 rows to determine whether to generate nextToken.
	template += fmt.Sprintf(statements.applyLimit, pageSize+1)

	return &spanner.Statement{
		SQL:    template,
		Params: params,
	}
}

func GetObservationsQuery(variables []string, entities []string) *spanner.Statement {
	stmt := &spanner.Statement{
		SQL:    statements.getObs,
		Params: map[string]interface{}{},
	}

	filters := []string{}
	if len(variables) > 0 {
		variableFilter, variableVal := getParamStatement("variable", variables)
		stmt.Params["variable"] = variableVal
		filters = append(filters, fmt.Sprintf(statements.selectVariableDcids, variableFilter))
	}
	if len(entities) > 0 {
		entityFilter, entityVal := getParamStatement("entity", entities)
		stmt.Params["entity"] = entityVal
		filters = append(filters, fmt.Sprintf(statements.selectEntityDcids, entityFilter))
	}
	stmt.SQL += where + strings.Join(filters, and)

	return stmt
}

func GetObservationsContainedInPlaceQuery(variables []string, containedInPlace *v2.ContainedInPlace) *spanner.Statement {
	stmt := GetObservationsQuery(variables, []string{} /*entities*/)
	stmt.SQL = fmt.Sprintf(statements.getObsByVariableAndContainedInPlace, stmt.SQL)
	stmt.Params["ancestor"] = containedInPlace.Ancestor
	stmt.Params["childPlaceType"] = containedInPlace.ChildPlaceType
	return stmt
}

func FilterStatVarsByEntityQuery(variables []string, entities []string) (*spanner.Statement, error) {
	if len(variables) == 0 && len(entities) == 0 {
		return nil, fmt.Errorf("FilterStatVarsByEntityQuery must be called with at least one variable or entity")
	}
	sql := statements.getStatVarsByEntity
	params := map[string]interface{}{}

	var filters []string
	if len(variables) > 0 {
		filter, val := getParamStatement("variables", variables)
		params["variables"] = val
		filters = append(filters, fmt.Sprintf("variable_measured %s", filter))
	}
	if len(entities) > 0 {
		filter, val := getParamStatement("entities", entities)
		params["entities"] = val
		filters = append(filters, fmt.Sprintf("observation_about %s", filter))
	}

	if len(filters) > 0 {
		sql += where + strings.Join(filters, and)
	}

	return &spanner.Statement{
		SQL:    sql,
		Params: params,
	}, nil
}

func SearchNodesQuery(query string, types []string) *spanner.Statement {
	params := map[string]interface{}{
		"query": query,
	}

	filterTypes := ""
	if len(types) > 0 {
		params["types"] = types
		filterTypes = statements.filterTypes
	}

	return &spanner.Statement{
		SQL:    fmt.Sprintf(statements.searchNodesByQuery, filterTypes, merger.MAX_SEARCH_RESULTS),
		Params: params,
	}
}

func ResolveByIDQuery(nodes []string, in, out string) *spanner.Statement {
	params := map[string]interface{}{
		"inProp":  in,
		"outProp": out,
	}

	var sql string
	if in == "dcid" {
		params["nodes"] = nodes
		if out == "dcid" { // DCID to DCID
			sql = statements.resolveDcidToDcid
		} else { // DCID to property
			sql = statements.resolveDcidToProp
		}
	} else {
		params["nodes"] = addObjectValues(nodes)
		if out == "dcid" { // Property to DCID
			sql = statements.resolvePropToDcid
		} else { // Property to property
			sql = statements.resolvePropToProp
		}
	}

	return &spanner.Statement{
		SQL:    sql,
		Params: params,
	}
}

func SparqlQuery(nodes []types.Node, queries []*types.Query, opts *types.QueryOptions) (*spanner.Statement, error) {
	sql := statements.graphPrefixAny
	params := map[string]interface{}{}

	safeAliasMap := generateSafeAliasMap(queries)
	count := 0
	triples := []string{}
	nodeMaps := []string{}
	for _, q := range queries {
		eCount := strconv.Itoa(count)
		params["predicate"+eCount] = q.Pred

		// Parse subject.
		sId := safeAliasMap[q.Sub.Alias]
		var sFilter string

		// Parse object.
		var oId, oFilter string
		if node, ok := q.Obj.(types.Node); ok {
			oId = safeAliasMap[node.Alias]
			if q.Pred == "dcid" {
				nodeMaps = append(nodeMaps, oId+" = "+sId)
			}
		} else {
			var vals []string
			switch v := q.Obj.(type) {
			case []string:
				vals = v
			case string:
				vals = []string{v}
			default:
				return nil, fmt.Errorf("unsupported object type: %T", q.Obj)
			}
			if q.Pred != "typeOf" && q.Pred != "dcid" { // typeOf has reference object.
				vals = addObjectValues(vals)
			}
			if q.Pred == "dcid" {
				sFilter = fmt.Sprintf(statements.nodeFilter, sId)
				params[sId] = vals
			} else {
				oId = "o" + eCount
				oFilter = fmt.Sprintf(statements.nodeFilter, oId)
				params[oId] = vals
			}

		}

		if q.Pred == "dcid" {
			if oId == "" {
				triples = append(triples, fmt.Sprintf(statements.node, sId, sFilter))
			} else {
				triples = append(triples, fmt.Sprintf(statements.node, oId, oFilter))
			}
		} else {
			triples = append(triples, fmt.Sprintf(statements.triple, sId, sFilter, count, oId, oFilter))
			count++
		}

	}

	sql += strings.Join(triples, ",\n\t\t")

	if len(nodeMaps) > 0 {
		sql += "\n\t\t" + sqlWhere + "\n\t\t\t" + strings.Join(nodeMaps, "\n\t\t\tAND ")
	}

	var nodeAliases []string
	for _, n := range nodes {
		alias := safeAliasMap[n.Alias]
		nodeAliases = append(nodeAliases, alias+".value AS "+alias)
	}
	var distinct string
	if opts.Distinct {
		distinct = sqlDistinct
	}
	sql += sqlReturn + distinct + "\n\t\t\t" + strings.Join(nodeAliases, ",\n\t\t\t")

	if opts.Orderby != "" {
		// Verify that the orderby alias exists.
		if _, ok := safeAliasMap[opts.Orderby]; !ok {
			return nil, fmt.Errorf("orderby alias %s not found", opts.Orderby)
		}
		sql += sqlOrderBy + "\n\t\t\t" + safeAliasMap[opts.Orderby]
		if !opts.ASC {
			sql += sqlDesc
		}
	}
	if opts.Limit > 0 {
		sql += sqlLimit
		params["limit"] = opts.Limit
	}

	return &spanner.Statement{
		SQL:    sql,
		Params: params,
	}, nil
}

func GetCacheDataQuery(typeFilter CacheDataType, keys []string) *spanner.Statement {
	keyFilter, keyVal := getParamStatement("key", keys)
	params := map[string]interface{}{
		"type": string(typeFilter),
		"key":  keyVal,
	}

	return &spanner.Statement{
		SQL:    fmt.Sprintf(statements.getCacheData, keyFilter),
		Params: params,
	}
}

// GetStatVarGroupNode returns a query to get StatVarGroupNode info.
func GetStatVarGroupNodeQuery(nodes []string) *spanner.Statement {
	nodeFilter, nodeVal := getParamStatement("nodes", nodes)

	selfFilter := statements.attachSVG
	if len(nodes) > 1 {
		selfFilter = statements.attachSVGs
	}

	return &spanner.Statement{
		SQL: fmt.Sprintf(statements.getStatVarGroupNode, nodeFilter, selfFilter),
		Params: map[string]interface{}{
			"nodes": nodeVal,
		},
	}
}

// GetSVGChildren returns a query to get all children for a given stat var group.
func GetSVGChildrenQuery(node string) *spanner.Statement {
	return &spanner.Statement{
		SQL: statements.getSVGChildren,
		Params: map[string]interface{}{
			"node": node,
		},
	}
}

// GetFilteredSVGChildren returns a query to get children for a given stat var group filtered by constrained entities and existence threshold.
func GetFilteredSVGChildrenQuery(template string, node string, constrainedPlaces []string, constrainedImport string, numEntitiesExistence int) *spanner.Statement {
	params := map[string]interface{}{
		"node":                 node,
		"numEntitiesExistence": numEntitiesExistence,
	}

	var entityFilter string
	var distinct string
	if constrainedImport != "" {
		entityFilter = statements.filterDescendentStatVarsByImport
		params["predicate"] = getImportFilterPredicate(constrainedImport)
		params["import"] = constrainedImport
		distinct = "e1.subject_id"
	}
	if len(constrainedPlaces) > 0 {
		placeFilter, placeVal := getParamStatement("places", constrainedPlaces)
		if entityFilter == "" {
			entityFilter = "\n\t\t\t\t" + sqlWhere + " " + fmt.Sprintf(statements.selectEntityDcids, placeFilter)
		} else {
			entityFilter = entityFilter + "\n\t\t\t\t\t" + sqlAnd + " " + fmt.Sprintf(statements.selectEntityDcids, placeFilter)
		}
		params["places"] = placeVal
		distinct = "observation_about"
	}

	var numFilter string
	if numEntitiesExistence > 1 {
		numFilter = fmt.Sprintf(statements.filterDescendentStatVarsByNumEntitiesExistence, distinct)
		params["numEntitiesExistence"] = numEntitiesExistence
	}

	var baseStatement string
	switch template {
	case templateSV:
		baseStatement = statements.getFilteredChildSVs
	case templateSVG:
		baseStatement = statements.getFilteredChildSVGs
	case templateTopic:
		baseStatement = statements.getFilteredTopic
	}

	return &spanner.Statement{
		SQL:    fmt.Sprintf(baseStatement, entityFilter, numFilter),
		Params: params,
	}
}

// generateSafeAliasMap generates a map of safe aliases for SPARQL queries.
func generateSafeAliasMap(queries []*types.Query) map[string]string {
	safeAliasMap := make(map[string]string)
	count := 0
	for _, q := range queries {
		if _, exists := safeAliasMap[q.Sub.Alias]; !exists {
			safeAliasMap[q.Sub.Alias] = fmt.Sprintf("a%d", count)
			count++
		}
		if node, ok := q.Obj.(types.Node); ok {
			if _, exists := safeAliasMap[node.Alias]; !exists {
				safeAliasMap[node.Alias] = fmt.Sprintf("a%d", count)
				count++
			}
		}
	}
	return safeAliasMap
}

func generateValueHash(input string) string {
	data := []byte(input)
	hash := sha256.Sum256(data)
	return base64.StdEncoding.EncodeToString(hash[:])
}

func generateObjectValue(input string) string {
	var prefix string
	if len(input) <= objectValuePrefix {
		prefix = input
	} else {
		i := objectValuePrefix
		for ; i > 0 && !utf8.RuneStart(input[i]); i-- {
		}
		prefix = input[:i]

	}
	return prefix + ":" + generateValueHash(input)
}

func addObjectValues(input []string) []string {
	result := make([]string, 0, len(input)*2)
	for _, v := range input {
		result = append(result, v)
		result = append(result, generateObjectValue(v))
	}
	return result
}

// getParamStatement returns the appropriate SQL statement and parameter value for filtering by a parameter based on the number of inputs.
func getParamStatement(param string, inputs []string) (string, interface{}) {
	if len(inputs) == 1 {
		return fmt.Sprintf(statements.getParam, param), inputs[0]
	}
	return fmt.Sprintf(statements.getParams, param), inputs
}

// getImportFilterPredicate returns the appropriate filter predicate for a given filter entity.
func getImportFilterPredicate(entity string) string {
	if strings.HasPrefix(entity, "dc/d/") {
		return predicateIsPartOf
	}
	return predicateSource
}

func GetEventCollectionDateQuery(placeID, eventType string) *spanner.Statement {
	return &spanner.Statement{
		SQL: statements.getEventCollectionDate,
		Params: map[string]interface{}{
			"placeID":   placeID,
			"eventType": eventType,
		},
	}
}

func GetEventCollectionDcidsQuery(placeID, eventType, date string) *spanner.Statement {
	if cfg, ok := EventConfigs[eventType]; ok && cfg.MagnitudeProp != "" {
		return &spanner.Statement{
			SQL: statements.getEventCollectionDcidsWithMagnitude,
			Params: map[string]interface{}{
				"placeID":       placeID,
				"eventType":     eventType,
				"date":          date,
				"magnitudeProp": cfg.MagnitudeProp,
			},
		}
	}
	return &spanner.Statement{
		SQL: statements.getEventCollectionDcids,
		Params: map[string]interface{}{
			"placeID":   placeID,
			"eventType": eventType,
			"date":      date,
		},
	}
}

// GetTermEmbeddingQuery returns a Spanner statement to extract embedding from a query.
func GetTermEmbeddingQuery(modelName, searchLabel, taskType string) *spanner.Statement {
	return &spanner.Statement{
		SQL: statements.getEmbeddingFromQuery,
		Params: map[string]interface{}{
			"model_name":   modelName,
			"search_label": searchLabel,
			"task_type":    taskType,
		},
	}
}

// VectorSearchQuery returns a Spanner statement to search nodes using vector similarity.
func VectorSearchQuery(limit int, embeddings []float64, numLeaves int, threshold float64) *spanner.Statement {
	optionsJSON := fmt.Sprintf(`{"num_leaves_to_search": %d}`, numLeaves)
	return &spanner.Statement{
		SQL: statements.vectorSearchNode,
		Params: map[string]interface{}{
			"embeddings":         embeddings,
			"limit":              limit,
			"options":            optionsJSON,
			"distance_threshold": threshold,
		},
	}
}
