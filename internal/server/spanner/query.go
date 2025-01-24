// Copyright 2024 Google LLC
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

// Queries executed by the SpannerClient.
package spanner

import (
	"context"
	"fmt"
	"strconv"

	"cloud.google.com/go/spanner"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"google.golang.org/api/iterator"
)

const (
	// Maximum number of edge hops to traverse for chained properties.
	MAX_HOPS = 10
)

// GetNodeProps retrieves node properties from Spanner given a list of IDs and a direction and returns a map.
func (sc *SpannerClient) GetNodeProps(ctx context.Context, ids []string, out bool) (map[string][]*Property, error) {
	props := map[string][]*Property{}
	if len(ids) == 0 {
		return props, nil
	}
	for _, id := range ids {
		props[id] = []*Property{}
	}

	var stmt spanner.Statement

	switch out {
	case true:
		stmt = spanner.Statement{
			SQL:    statements.getPropsBySubjectID,
			Params: map[string]interface{}{"ids": ids},
		}
	case false:
		stmt = spanner.Statement{
			SQL:    statements.getPropsByObjectID,
			Params: map[string]interface{}{"ids": ids},
		}
	}

	err := sc.queryAndCollect(
		ctx,
		stmt,
		func() interface{} {
			return &Property{}
		},
		func(rowStruct interface{}) {
			prop := rowStruct.(*Property)
			subjectID := prop.SubjectID
			props[subjectID] = append(props[subjectID], prop)
		},
	)
	if err != nil {
		return props, err
	}

	return props, nil
}

// GetNodeEdgesByID retrieves node edges from Spanner given a list of IDs and a property Arc and returns a map.
func (sc *SpannerClient) GetNodeEdgesByID(ctx context.Context, ids []string, arc *v2.Arc) (map[string][]*Edge, error) {
	// TODO: Support pagination.
	edges := make(map[string][]*Edge)
	if len(ids) == 0 {
		return edges, nil
	}
	for _, id := range ids {
		edges[id] = []*Edge{}
	}

	// Validate input.
	if arc.Decorator != "" && (arc.SingleProp == "" || arc.SingleProp == WILDCARD || len(arc.BracketProps) > 0) {
		return nil, fmt.Errorf("chain expressions are only supported for a single property")
	}

	params := map[string]interface{}{"ids": ids}

	// Attach property arcs.
	filterProps := ""
	if arc.SingleProp != "" && arc.SingleProp != WILDCARD {
		filterProps = statements.filterProps
		params["props"] = []string{arc.SingleProp}
	} else if len(arc.BracketProps) > 0 {
		filterProps = statements.filterProps
		params["props"] = arc.BracketProps
	}

	var template string
	switch arc.Out {
	case true:
		if arc.Decorator == CHAIN {
			template = statements.getChainedEdgesBySubjectID
			params["predicate"] = arc.SingleProp
			params["result_predicate"] = arc.SingleProp + arc.Decorator
		} else {
			template = fmt.Sprintf(statements.getEdgesBySubjectID, filterProps)
		}
	case false:
		if arc.Decorator == CHAIN {
			template = statements.getChainedEdgesByObjectID
			params["predicate"] = arc.SingleProp
			params["result_predicate"] = arc.SingleProp + arc.Decorator
		} else {
			template = fmt.Sprintf(statements.getEdgesByObjectID, filterProps)
		}
	}

	// Attach filters.
	i := 0
	for prop, val := range arc.Filter {
		template += fmt.Sprintf(statements.filterObjects, i)
		params["prop"+strconv.Itoa(i)] = prop
		params["val"+strconv.Itoa(i)] = val
		i += 1
	}

	stmt := spanner.Statement{
		SQL:    template,
		Params: params,
	}

	err := sc.queryAndCollect(
		ctx,
		stmt,
		func() interface{} {
			return &Edge{}
		},
		func(rowStruct interface{}) {
			edge := rowStruct.(*Edge)
			subjectID := edge.SubjectID
			edges[subjectID] = append(edges[subjectID], edge)
		},
	)
	if err != nil {
		return nil, err
	}

	return edges, nil
}

// GetObservations retrieves observations from Spanner given a list of variables and entities.
func (sc *SpannerClient) GetObservations(ctx context.Context, variables []string, entities []string) ([]*Observation, error) {
	var observations []*Observation
	if len(variables) == 0 || len(entities) == 0 {
		return observations, nil
	}

	stmt := spanner.Statement{
		SQL: statements.getObsByVariableAndEntity,
		Params: map[string]interface{}{
			"variables": variables,
			"entities":  entities,
		},
	}

	err := sc.queryAndCollect(
		ctx,
		stmt,
		func() interface{} {
			return &Observation{}
		},
		func(rowStruct interface{}) {
			observation := rowStruct.(*Observation)
			observations = append(observations, observation)
		},
	)
	if err != nil {
		return observations, err
	}

	return observations, nil
}

// GetObservationsContainedInPlace retrieves observations from Spanner given a list of variables and an entity expression.
func (sc *SpannerClient) GetObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace) ([]*Observation, error) {
	var observations []*Observation
	if len(variables) == 0 || containedInPlace == nil {
		return observations, nil
	}

	stmt := spanner.Statement{
		SQL: statements.getObsByVariableAndContainedInPlace,
		Params: map[string]interface{}{
			"variables":      variables,
			"ancestor":       containedInPlace.Ancestor,
			"childPlaceType": containedInPlace.ChildPlaceType,
		},
	}

	err := sc.queryAndCollect(
		ctx,
		stmt,
		func() interface{} {
			return &Observation{}
		},
		func(rowStruct interface{}) {
			observation := rowStruct.(*Observation)
			observations = append(observations, observation)
		},
	)
	if err != nil {
		return observations, err
	}

	return observations, nil
}

// SearchNodes searches nodes in the graph based on the query and optionally the types.
// If the types array is empty, it searches across nodes of all types.
// A maximum of 100 results are returned.
func (sc *SpannerClient) SearchNodes(ctx context.Context, query string, types []string) ([]*SearchNode, error) {
	var nodes []*SearchNode
	if query == "" {
		return nodes, nil
	}

	var stmt spanner.Statement

	if len(types) == 0 {
		stmt = spanner.Statement{
			SQL: statements.searchNodesByQuery,
			Params: map[string]interface{}{
				"query": query,
			},
		}
	} else {
		stmt = spanner.Statement{
			SQL: statements.searchNodesByQueryAndTypes,
			Params: map[string]interface{}{
				"query": query,
				"types": types,
			},
		}
	}

	err := sc.queryAndCollect(
		ctx,
		stmt,
		func() interface{} {
			return &SearchNode{}
		},
		func(rowStruct interface{}) {
			node := rowStruct.(*SearchNode)
			nodes = append(nodes, node)
		},
	)
	if err != nil {
		return nodes, err
	}

	return nodes, nil
}

func (sc *SpannerClient) queryAndCollect(
	ctx context.Context,
	stmt spanner.Statement,
	newStruct func() interface{},
	withStruct func(interface{}),
) error {
	iter := sc.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to fetch row: %w", err)
		}

		rowStruct := newStruct()
		if err := row.ToStructLenient(rowStruct); err != nil {
			return fmt.Errorf("failed to parse row: %w", err)
		}
		withStruct(rowStruct)
	}

	return nil
}
