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

// SQL / GQL statements executed by the SpannerClient
var statements = struct {
	// Fetch Properties for out arcs
	getPropsBySubjectID string
	// Fetch Properties for in arcs
	getPropsByObjectID string
	// Fetch Edges for out arcs with a single hop
	getEdgesBySubjectID string
	// Fetch Edges for out arcs with chaining
	getChainedEdgesBySubjectID string
	// Fetch Edges for in arcs with a single hop
	getEdgesByObjectID string
	// Fetch Edges for in arcs with chaining
	getChainedEdgesByObjectID string
	// Subquery to filter edges by predicate
	filterProps string
	// Subquery to filter edges by object property-values
	filterObjects string
	// Fetch Observations for variable+entity.
	getObsByVariableAndEntity string
}{
	getPropsBySubjectID: `
	SELECT
		DISTINCT subject_id,
		predicate
	FROM 
		Edge
	WHERE
		subject_id IN UNNEST(@ids)
	`,
	getPropsByObjectID: `
	SELECT
		DISTINCT object_id AS subject_id,
		predicate
	FROM
		Edge
	WHERE
		object_id IN UNNEST(@ids)
		AND object_value IS NULL
	`,
	getEdgesBySubjectID: `
	SELECT
		result.subject_id,
		result.predicate,
		COALESCE(result.object_id, '') AS object_id,
		COALESCE(result.object_value, '') AS object_value,
		COALESCE(result.provenance, '') AS provenance,
		COALESCE(result.name, '') AS name,
		COALESCE(result.types, []) AS types
	FROM (
		SELECT
			*
		FROM
			GRAPH_TABLE (
				DCGRAPH MATCH -[e:Edge
				WHERE
					e.subject_id IN UNNEST(@ids)
					AND e.object_value IS NULL
					AND e.subject_id != e.object_id%[1]s]->(n:Node)
				RETURN e.subject_id,
					e.predicate,
					e.object_id,
					'' as object_value,
					e.provenance,
					n.name,
					n.types
			)
		UNION ALL
		SELECT
			*
		FROM
			GRAPH_TABLE (
				DCGraph MATCH -[e:Edge
				WHERE
					e.subject_id IN UNNEST(@ids)
					AND e.object_value IS NOT NULL%[1]s]-> 
				RETURN e.subject_id,
					e.predicate,
					'' as object_id,
					e.object_value,
					e.provenance,
					'' AS name,
					ARRAY<STRING>[] AS types
			)
	)result
	`,
	getChainedEdgesBySubjectID: `
	SELECT
		result.subject_id,
		@result_predicate AS predicate,
		COALESCE(result.object_id, '') AS object_id,
		COALESCE(result.object_value, '') AS object_value,
		'' AS provenance,
		COALESCE(result.name, '') AS name,
		ARRAY<STRING>[] AS types
	FROM (
		SELECT
			*
		FROM
			GRAPH_TABLE (
				DCGRAPH MATCH (m:Node
				WHERE
					m.subject_id IN UNNEST(@ids))-[e:Edge
				WHERE
					e.predicate = @predicate]->{1,%d}(n:Node)
				WHERE 
					m != n
				RETURN DISTINCT m.subject_id,
					n.subject_id as object_id,
					'' as object_value,
					n.name
			)
		UNION ALL
		SELECT
			*
		FROM
			GRAPH_TABLE (
				DCGraph MATCH -[e:Edge
				WHERE
					e.subject_id IN UNNEST(@ids)
					AND e.object_value IS NOT NULL
					AND e.predicate = @predicate]-> 
				RETURN e.subject_id,
					'' AS object_id,
					e.object_value,
					'' AS name
			)
	)result
	`,
	getEdgesByObjectID: `
	SELECT
		result.subject_id,
		result.predicate,
		result.object_id,
		'' AS object_value,
		COALESCE(result.provenance, '') AS provenance,
		COALESCE(result.name, '') AS name,
		COALESCE(result.types, []) AS types,
	FROM
		GRAPH_TABLE (
			DCGraph MATCH <-[e:Edge
			WHERE
				e.object_id IN UNNEST(@ids)
				AND e.subject_id != e.object_id%s]-(n:Node) 
			RETURN e.object_id AS subject_id,
				e.predicate,
				e.subject_id AS object_id,
				e.provenance,
				n.name,
				n.types
	)result
	`,
	getChainedEdgesByObjectID: `
	SELECT
		result.subject_id,
		@result_predicate AS predicate,
		result.object_id,
		'' AS object_value,
		'' AS provenance,
		COALESCE(result.name, '') AS name,
		ARRAY<STRING>[] AS types
	FROM
		GRAPH_TABLE (
			DCGraph MATCH (m:Node
			WHERE m.subject_id IN UNNEST(@ids))<-[e:Edge
		WHERE
			e.predicate = @predicate]-{1,%d}(n:Node) 
		WHERE
			m!= n	
		RETURN DISTINCT m.subject_id,
			n.subject_id AS object_id,
			n.name
		)result
	`,
	filterProps: `
	AND e.predicate IN UNNEST(@props)
	`,
	filterObjects: `
	INNER JOIN (
		SELECT 
			*
		FROM
			GRAPH_TABLE (
				DCGraph MATCH -[e:Edge 
				WHERE
					e.predicate = @prop%[1]d
					AND e.object_id IN UNNEST(@val%[1]d)]-> 
				RETURN e.subject_id
			)
		UNION DISTINCT
		SELECT
			*
		FROM
			GRAPH_TABLE (
				DCGraph MATCH -[e:Edge
				WHERE
					e.predicate = @prop%[1]d
					AND e.object_value IN UNNEST(@val%[1]d)]->
				RETURN e.subject_id
			) 			
	)filter%[1]d
	ON 
		result.object_id = filter%[1]d.subject_id
	`,
	getObsByVariableAndEntity: `
		SELECT
			variable_measured,
			observation_about,
			observations,
			provenance,
			COALESCE(observation_period, '') AS observation_period,
			COALESCE(measurement_method, '') AS measurement_method,
			COALESCE(unit, '') AS unit,
			COALESCE(scaling_factor, '') AS scaling_factor,
			import_name,
			provenance_url
		FROM Observation
		WHERE
			variable_measured IN UNNEST(@variables) AND
			observation_about IN UNNEST(@entities)
	`,
}

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
			template = fmt.Sprintf(statements.getChainedEdgesBySubjectID, MAX_HOPS)
			params["predicate"] = arc.SingleProp
			params["result_predicate"] = arc.SingleProp + arc.Decorator
		} else {
			template = fmt.Sprintf(statements.getEdgesBySubjectID, filterProps)
		}
	case false:
		if arc.Decorator == CHAIN {
			template = fmt.Sprintf(statements.getChainedEdgesByObjectID, MAX_HOPS)
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
		return edges, err
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
