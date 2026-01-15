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
	// Prefix for graph queries with any node selection.
	sqlReturn = "\n\t\tRETURN"
	// DISTINCT keyword for SQL queries.
	sqlDistinct = " DISTINCT "
	// DESC keyword for SQL queries.
	sqlDesc = "\n\t\tDESC"
	// ORDER BY clause for SQL queries.
	sqlOrderBy = "\n\t\tORDER BY "
	// LIMIT clause for SQL queries.
	sqlLimit = "\n\t\tLIMIT "
)

const (
	// Prefix length of value to include in object value ids.
	objectValuePrefix = 16
)

func GetCompletionTimestampQuery() *spanner.Statement {
	return &spanner.Statement{
		SQL: statements.getCompletionTimestamp,
	}
}

func GetNodePropsQuery(ids []string, out bool) *spanner.Statement {
	switch out {
	case true:
		return &spanner.Statement{
			SQL:    statements.getPropsBySubjectID,
			Params: map[string]interface{}{"ids": ids},
		}
	default:
		return &spanner.Statement{
			SQL:    statements.getPropsByObjectID,
			Params: map[string]interface{}{"ids": ids},
		}
	}
}

func GetNodeEdgesByIDQuery(ids []string, arc *v2.Arc, pageSize, offset int) *spanner.Statement {
	params := map[string]interface{}{"ids": ids}

	// Attach predicates.
	filterPredicate := ""
	if arc.SingleProp != "" && arc.SingleProp != v3.Wildcard && arc.Decorator != v3.Chain {
		filterPredicate = statements.filterPredicate
		params["predicates"] = []string{arc.SingleProp}
	} else if len(arc.BracketProps) > 0 {
		filterPredicate = statements.filterPredicate
		params["predicates"] = arc.BracketProps
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
				objectFilter = fmt.Sprintf(statements.filterValue, i)
				params["val"+strconv.Itoa(i)] = filterVal
			}
			subqueries = append(subqueries, fmt.Sprintf(statements.filterProperty, i, objectFilter))
			i += 1
		}
	}

	// Order subqueries by selectivity (i.e. expected cardinality of edges) for query performance.
	var subquery string
	switch arc.Out {
	case true:
		if arc.Decorator == v3.Chain {
			subquery = fmt.Sprintf(statements.getChainedEdgesBySubjectID, maxHops)
			params["predicate"] = arc.SingleProp
			params["result_predicate"] = arc.SingleProp + arc.Decorator
		} else {
			subquery = fmt.Sprintf(statements.getEdgesBySubjectID, filterPredicate)
		}
		// Add filters last for out-edges.
		subqueries = append([]string{subquery}, subqueries...)
	case false:
		if arc.Decorator == v3.Chain {
			subquery = fmt.Sprintf(statements.getChainedEdgesByObjectID, maxHops)
			params["predicate"] = arc.SingleProp
			params["result_predicate"] = arc.SingleProp + arc.Decorator
		} else {
			subquery = fmt.Sprintf(statements.getEdgesByObjectID, filterPredicate)
		}
		// Add filters first for in-edges.
		subqueries = append(subqueries, subquery)
	}

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
		stmt.Params["variables"] = variables
		filters = append(filters, statements.selectVariableDcids)
	}
	if len(entities) > 0 {
		stmt.Params["entities"] = entities
		filters = append(filters, statements.selectEntityDcids)
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

	aliasToDcid := mapAliasToDcid(queries)
	count := 0
	triples := []string{}
	dcids := map[string]bool{}
	aliases := map[string]bool{}
	for _, q := range queries {
		// Skip dcid triples which will be resolved.
		if q.Pred == "dcid" {
			continue
		}

		eCount := strconv.Itoa(count)
		params["predicate"+eCount] = q.Pred

		// Parse subject.
		aliases[q.Sub.Alias] = true
		sId := getAlias(q.Sub)
		var sFilter string
		if dcid, ok := aliasToDcid[q.Sub.Alias]; ok {
			if _, ok := dcids[dcid]; !ok {
				sFilter = fmt.Sprintf(statements.nodeFilter, sId)
				params[sId] = []string{dcid}
				dcids[dcid] = true
			}
		}

		// Parse object.
		var oId, oFilter string
		if node, ok := q.Obj.(types.Node); ok {
			aliases[node.Alias] = true
			oId = getAlias(node)
			if dcid, ok := aliasToDcid[node.Alias]; ok {
				if _, ok := dcids[dcid]; !ok {
					oFilter = fmt.Sprintf(statements.nodeFilter, oId)
					params[oId] = []string{dcid}
					dcids[dcid] = true
				}
			}
		} else {
			oId = "o" + eCount
			var vals []string
			switch v := q.Obj.(type) {
			case []string:
				vals = v
			case string:
				vals = []string{v}
			default:
				return nil, fmt.Errorf("unsupported object type: %T", q.Obj)
			}
			if q.Pred != "typeOf" { // typeOf has reference object.
				vals = addObjectValues(vals)
			}
			oFilter = fmt.Sprintf(statements.nodeFilter, oId)
			params[oId] = vals
		}

		triples = append(triples, fmt.Sprintf(statements.triple, sId, sFilter, count, oId, oFilter))
		count++
	}

	sql += strings.Join(triples, ",\n\t\t")

	var nodeAliases []string
	for _, n := range nodes {
		nodeAliases = append(nodeAliases, getAlias(n)+".value")
	}
	var distinct string
	if opts.Distinct {
		distinct = sqlDistinct
	}
	sql += sqlReturn + distinct + "\n\t\t\t" + strings.Join(nodeAliases, ",\n\t\t\t")

	if opts.Orderby != "" {
		// Verify that the orderby alias exists.
		if _, ok := aliases[opts.Orderby]; !ok {
			return nil, fmt.Errorf("orderby alias %s not found", opts.Orderby)
		}
		sql += sqlOrderBy + "\n\t\t\t" + opts.Orderby[1:] + "_.value"
		if !opts.ASC {
			sql += sqlDesc
		}
	}
	if opts.Limit > 0 {
		sql += sqlLimit + strconv.Itoa(opts.Limit)
	}

	return &spanner.Statement{
		SQL:    sql,
		Params: params,
	}, nil
}

// mapAliasToDcid maps SPARQL aliases to their corresponding DCIDs.
func mapAliasToDcid(queries []*types.Query) map[string]string {
	dcidMap := make(map[string]string)
	for _, q := range queries {
		if q.Pred == "dcid" {
			dcidMap[q.Sub.Alias] = q.Obj.(string)
		}
	}
	return dcidMap
}

// getAlias returns a SPARQL alias for Spanner.
func getAlias(node types.Node) string {
	return node.Alias[1:] + "_"
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
