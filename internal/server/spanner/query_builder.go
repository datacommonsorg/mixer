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
	"fmt"
	"sort"
	"strconv"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/datacommonsorg/mixer/internal/merger"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
)

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
	if arc.SingleProp != "" && arc.SingleProp != WILDCARD && arc.Decorator != CHAIN {
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
			filterVal := addValueHashes(arc.Filter[prop])
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
		if arc.Decorator == CHAIN {
			subquery = fmt.Sprintf(statements.getChainedEdgesBySubjectID, MAX_HOPS)
			params["predicate"] = arc.SingleProp
			params["result_predicate"] = arc.SingleProp + arc.Decorator
		} else {
			subquery = fmt.Sprintf(statements.getEdgesBySubjectID, filterPredicate)
		}
		// Add filters last for out-edges.
		subqueries = append([]string{subquery}, subqueries...)
	case false:
		if arc.Decorator == CHAIN {
			subquery = fmt.Sprintf(statements.getChainedEdgesByObjectID, MAX_HOPS)
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
	case CHAIN:
		prefix = statements.chainedEdgePrefix
		returnEdges = statements.returnChainedEdges
	default:
		prefix = statements.edgePrefix
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
	stmt.SQL += WHERE + strings.Join(filters, AND)

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
		params["nodes"] = addValueHashes(nodes)
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
