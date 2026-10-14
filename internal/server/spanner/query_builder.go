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
)

const (
	// Prefix length of value to include in object value ids.
	objectValuePrefix = 16
	// Template for fetching child SVs
	templateSV = "SV"
	// Template for fetching child SVGs
	templateSVG = "SVG"
	// isPartOf predicate
	predicateIsPartOf = "isPartOf"
	// source predicate
	predicateSource = "source"
	// memberOf predicate
	predicateMemberOf = "memberOf"
	// specializationOf predicate
	predicateSpecializationOf = "specializationOf"
	// Dataset dcid prefix
	prefixDataset = "dc/d/"
	// Source dcid prefix
	prefixSource = "dc/s/"
	// StatVarGroup dcid prefix
	prefixSVG = "dc/g/"
	// key for filtering nodes by language suffix
	filterLang = "$lang"
)

// Internal-only (generated) predicates that should not be returned via Node APIs unless specifically requested.
var blocklistEdgePredicates = []string{
	"linkedContainedInPlace",
	"linkedMemberOf",
	"linkedMember",
}

// QueryOperatorHandler defines the signature for processing meta-filters.
// It returns the generated SQL condition and mutates the params map directly.
type QueryOperatorHandler func(prefix string, values []string, params map[string]interface{}) string

// queryOperatorRegistry maps operator keys to their respective handlers.
// Add future operators (e.g., "$date", "$limit") here.
var queryOperatorRegistry = map[string]QueryOperatorHandler{
	filterLang: handleLangOperator,
}

// handleLangOperator generates the SQL clause for language matching.
func handleLangOperator(prefix string, langs []string, params map[string]interface{}) string {
	if len(langs) == 0 {
		return ""
	}
	var parts []string
	for j, lang := range langs {
		paramName := fmt.Sprintf("%s_%d", prefix, j)
		// Database stores values exactly like "Value"@en
		params[paramName] = "@" + strings.ToLower(lang)
		parts = append(parts, fmt.Sprintf("ENDS_WITH(n.value, @%s)", paramName))
	}
	return strings.Join(parts, " OR ")
}

func GetCompletionTimestampQuery(useNewSchema bool) *spanner.Statement {
	sql := statements.getCompletionTimestamp
	if useNewSchema {
		sql = statements.getIngestionHistoryTimestamp
	}
	return &spanner.Statement{
		SQL: sql,
	}
}

func GetNodePropsQuery(ids []string, out bool, queryConfigs ...QueryConfig) *spanner.Statement {
	var queryConfig QueryConfig
	if len(queryConfigs) > 0 {
		queryConfig = queryConfigs[0]
	}
	hint := statements.graphColumnarScanHint
	if queryConfig.SpannerEmulatorCompatibility {
		hint = ""
	}

	idFilter, idVal := getParamStatement("id", ids)
	params := map[string]interface{}{
		"id": idVal,
	}

	var filterPredicate string
	if len(blocklistEdgePredicates) > 0 {
		filterPredicate = statements.filterPredicateBlocklist
	}

	switch out {
	case true:
		return &spanner.Statement{
			SQL:    hint + fmt.Sprintf(statements.getPropsBySubjectID, idFilter, filterPredicate),
			Params: params,
		}
	default:
		return &spanner.Statement{
			SQL:    hint + fmt.Sprintf(statements.getPropsByObjectID, idFilter, filterPredicate),
			Params: params,
		}
	}
}

// GetNodeEdgesByIDQuery plans and builds a Node edge query.
func GetNodeEdgesByIDQuery(
	ids []string,
	arc *v2.Arc,
	pageSize, offset int,
	queryConfig QueryConfig,
) (*spanner.Statement, error) {
	plan, err := planNodeQuery(arc, queryConfig)
	if err != nil {
		return nil, err
	}
	return buildNodeEdgesByIDQuery(ids, arc, pageSize, offset, plan)
}

func buildNodeEdgesByIDQuery(
	ids []string,
	arc *v2.Arc,
	pageSize, offset int,
	plan nodeQueryPlan,
) (*spanner.Statement, error) {
	useContainedInPlaceAncestorFirst := false
	switch plan.kind {
	case nodeQueryGeneric:
	case nodeQueryContainedInPlace:
		switch plan.containedInPlace.accessPath {
		case containedInPlaceLinkedEdge:
			return buildContainedInPlaceQuery(ids, arc, pageSize, offset, plan), nil
		case containedInPlaceTypeFirst:
		case containedInPlaceAncestorFirst:
			useContainedInPlaceAncestorFirst = true
		default:
			return nil, fmt.Errorf("unsupported contained-in-place access path: %d", plan.containedInPlace.accessPath)
		}
	default:
		return nil, fmt.Errorf("unsupported node query kind: %d", plan.kind)
	}

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
	} else if len(blocklistEdgePredicates) > 0 { // Use default blocklist.
		filterPredicate = statements.filterPredicateBlocklist
	}

	// Generate filters.
	subqueries := []string{}
	var filterParts []string // Holds the WHERE clauses for query operators

	if len(arc.Filter) > 0 {
		// Sort for determinism.
		props := make([]string, 0, len(arc.Filter))
		for prop := range arc.Filter {
			props = append(props, prop)
		}
		sort.Strings(props)

		i := 0
		for _, prop := range props {
			// 1. Dispatch to Query Operator Handler (if it exists)
			if handler, isOperator := queryOperatorRegistry[prop]; isOperator {
				safeOpName := strings.TrimPrefix(prop, "$") // e.g., "lang"
				prefix := fmt.Sprintf("op_global_%s", safeOpName)

				if clause := handler(prefix, arc.Filter[prop], params); clause != "" {
					filterParts = append(filterParts, "("+clause+")")
				}
				continue // Skip standard property logic
			}

			// 2. Standard Property Processing
			params["prop"+strconv.Itoa(i)] = prop
			objectFilter := ""
			filterVal := objectFilterValues(prop, arc.Filter[prop])
			if len(filterVal) > 0 {
				objectFilter = fmt.Sprintf(statements.filterValues, i)
				params["val"+strconv.Itoa(i)] = filterVal
			}
			indexHint := ""
			if len(filterVal) > 0 {
				indexHint = "@{FORCE_INDEX=InEdge}"
			}
			subqueries = append(subqueries, fmt.Sprintf(statements.filterProperty, i, indexHint, objectFilter))
			i++
		}
	}

	// Property-specific filters (from arc.BracketFilters)
	if len(arc.BracketFilters) > 0 {
		// Sort properties for deterministic SQL generation
		sortedProps := make([]string, 0, len(arc.BracketFilters))
		for prop := range arc.BracketFilters {
			sortedProps = append(sortedProps, prop)
		}
		sort.Strings(sortedProps)

		for i, prop := range sortedProps {
			propFilters := arc.BracketFilters[prop]
			propParam := fmt.Sprintf("prop_filter_%d", i)
			params[propParam] = prop

			// Sort operator keys to guarantee deterministic SQL output
			opKeys := make([]string, 0, len(propFilters))
			for k := range propFilters {
				opKeys = append(opKeys, k)
			}
			sort.Strings(opKeys)

			for _, opKey := range opKeys {
				if handler, isOperator := queryOperatorRegistry[opKey]; isOperator {
					safeOpName := strings.TrimPrefix(opKey, "$")
					prefix := fmt.Sprintf("op_prop_%d_%s", i, safeOpName)

					if clause := handler(prefix, propFilters[opKey], params); clause != "" {
						filterParts = append(filterParts, fmt.Sprintf("(e.predicate != @%s OR %s)", propParam, clause))
					}
				}
			}
		}
	}

	var nodeFilterStr string
	if len(filterParts) > 0 {
		nodeFilterStr = " WHERE " + strings.Join(filterParts, " AND ")
	}

	var subquery string
	switch arc.Out {
	case true:
		if arc.Decorator == v3.Chain {
			subquery = fmt.Sprintf(statements.getChainedEdgesBySubjectID, idFilter, maxHops, nodeFilterStr)
			params["predicate"] = arc.SingleProp
			params["result_predicate"] = arc.SingleProp + arc.Decorator
		} else {
			subquery = fmt.Sprintf(statements.getEdgesBySubjectID, idFilter, filterPredicate, nodeFilterStr)
		}
	case false:
		if arc.Decorator == v3.Chain {
			subquery = fmt.Sprintf(statements.getChainedEdgesByObjectID, idFilter, maxHops, nodeFilterStr)
			params["predicate"] = arc.SingleProp
			params["result_predicate"] = arc.SingleProp + arc.Decorator
		} else {
			subquery = fmt.Sprintf(statements.getEdgesByObjectID, idFilter, filterPredicate, nodeFilterStr)
		}
	}
	subqueries = append([]string{subquery}, subqueries...)

	// Pagination.
	pagination := getNodeQueryPagination(pageSize, offset)

	// Generate prefix and return statement.
	hint := statements.graphColumnarScanHint
	if plan.spannerEmulatorCompatibility {
		hint = ""
	}
	var prefix, returnEdges string
	switch arc.Decorator {
	case v3.Chain:
		prefix = hint + statements.graphPrefixAny
		returnEdges = fmt.Sprintf(statements.returnChainedEdges, pagination)
	default:
		prefix = hint + statements.graphPrefix
		if len(arc.Filter) > 0 {
			returnEdges += fmt.Sprintf(statements.returnFilterEdges, pagination)
		} else {
			returnEdges = fmt.Sprintf(statements.returnEdges, pagination)
		}
	}
	separator := statements.graphPatternSeparator
	if useContainedInPlaceAncestorFirst {
		separator = statements.graphPatternForceJoinOrder
	}
	template := prefix + strings.Join(subqueries, separator)
	template += returnEdges

	return &spanner.Statement{
		SQL:    template,
		Params: params,
	}, nil
}

// buildContainedInPlaceQuery returns a query for fetching containedInPlace edges.
func buildContainedInPlaceQuery(
	ids []string,
	arc *v2.Arc,
	pageSize, offset int,
	plan nodeQueryPlan,
) *spanner.Statement {
	params := map[string]interface{}{
		"ancestor": ids,
		// Filter has already been validated.
		"child_type": arc.Filter["typeOf"],
	}
	hint := statements.graphColumnarScanHint
	if plan.spannerEmulatorCompatibility {
		hint = ""
	}
	pagination := getNodeQueryPagination(pageSize, offset)
	sql := hint + fmt.Sprintf(statements.getEdgesContainedInPlace, pagination)
	return &spanner.Statement{
		SQL:    sql,
		Params: params,
	}

}

func getNodeQueryPagination(pageSize, offset int) string {
	query := ""
	if offset > 0 {
		query += fmt.Sprintf(statements.applyOffset, offset)
	}
	// Request pageSize+1 rows to determine whether to generate nextToken.
	return query + fmt.Sprintf(statements.applyLimit, pageSize+1)
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
			if q.Pred != "dcid" {
				vals = objectFilterValues(q.Pred, vals)
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

// GetKeyValueStoreQuery builds a query targeting KeyValueStore.
func GetKeyValueStoreQuery(typeFilter KeyValueStoreType, keys []string) *spanner.Statement {
	keyFilter, keyVal := getParamStatement("key", keys)
	params := map[string]interface{}{
		"type": string(typeFilter),
		"key":  keyVal,
	}

	return &spanner.Statement{
		SQL:    fmt.Sprintf(statements.getKeyValueStoreData, keyFilter),
		Params: params,
	}
}

// GetSVGChildren returns a query to get all children for a given stat var group.
func GetSVGChildrenQuery(node string, includeDefinitions bool) *spanner.Statement {
	sqlTemplate := statements.getSVGChildren
	if includeDefinitions {
		sqlTemplate = statements.getSVGChildrenWithDefinitions
	}
	return &spanner.Statement{
		SQL: sqlTemplate,
		Params: map[string]interface{}{
			"node": node,
		},
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

func objectFilterValues(predicate string, input []string) []string {
	if predicate == predTypeOf {
		return input
	}
	return addObjectValues(input)
}

// getParamStatement returns the appropriate SQL statement and parameter value for filtering by a parameter.
func getParamStatement(param string, inputs []string) (string, interface{}) {
	return fmt.Sprintf(statements.getParams, param), inputs
}

// getImportFilterPredicate returns the appropriate filter predicate for a given filter entity.
func getImportFilterPredicate(entity string) string {
	if strings.HasPrefix(entity, prefixDataset) {
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

// VectorSearchQuery returns a Spanner statement to search nodes using vector similarity.
func VectorSearchQuery(tableName string, limit int, embeddings []float64, numLeaves int, threshold float64, nodeTypes []string, embeddingLabel string) *spanner.Statement {
	optionsJSON := fmt.Sprintf(`{"num_leaves_to_search": %d}`, numLeaves)
	params := map[string]interface{}{
		"embeddings":      embeddings,
		"limit":           limit,
		"embedding_label": embeddingLabel,
	}
	var nodeTypeCondition string
	if len(nodeTypes) > 0 {
		nodeTypeCondition = statements.nodeTypeExistence
		params["node_types"] = nodeTypes
	}
	return &spanner.Statement{
		SQL:    fmt.Sprintf(statements.vectorSearchNode, "`"+tableName+"`", optionsJSON, fmt.Sprintf("%.2f", threshold), nodeTypeCondition),
		Params: params,
	}
}

// FilterNodesByTypesQuery returns a Spanner statement to filter nodes by type.
func FilterNodesByTypesQuery(nodes []string, typeFilters []string) *spanner.Statement {
	return &spanner.Statement{
		SQL: statements.filterNodesByTypes,
		Params: map[string]interface{}{
			"nodes":        nodes,
			"type_filters": typeFilters,
		},
	}
}
