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

	"cloud.google.com/go/spanner"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"google.golang.org/api/iterator"
)

// SQL / GQL statements executed by the SpannerClient
var statements = struct {
	getPropsBySubjectID       string
	getPropsByObjectID        string
	getEdgesBySubjectID       string
	getEdgesByObjectID        string
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
		edge.subject_id,
		edge.predicate,
		COALESCE(edge.object_id, '') AS object_id,
		COALESCE(edge.object_value, '') AS object_value,
		COALESCE(edge.provenance, '') AS provenance,
		COALESCE(object.name, '') AS name,
		COALESCE(object.types, []) AS types
	FROM
		Edge edge
	LEFT JOIN
		GRAPH_TABLE( DCGraph MATCH -[e:Edge
		WHERE
			e.subject_id IN UNNEST(@ids)
			AND e.object_value IS NULL]->(n:Node) RETURN n.subject_id,
			n.name,
			n.types) object
	ON
		edge.object_id = object.subject_id
	WHERE
		edge.subject_id IN UNNEST(@ids)
	`,
	getEdgesByObjectID: `
	GRAPH DCGraph MATCH (n:Node)-[e:Edge
	WHERE
		e.object_id IN UNNEST(@ids)
		AND e.subject_id != e.object_id]-> return e.object_id AS subject_id,
		e.predicate,
		n.subject_id AS object_id,
		'' as object_value,
		COALESCE(e.provenance, '') AS provenance,
		COALESCE(n.name, '') AS name,
		COALESCE(n.types, []) AS types
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

// GetNodeProps retrieves node properties from Spanner given a list of IDs and a direction.
func (sc *SpannerClient) GetNodeProps(ctx context.Context, ids []string, out bool) ([]*Property, error) {
	props := []*Property{}
	if len(ids) == 0 {
		return props, nil
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
			props = append(props, prop)
		},
	)
	if err != nil {
		return props, err
	}

	return props, nil
}

// GetNodeEdgesByID retrieves node edges from Spanner given a list of IDs and a property Arc and returns a map.
func (sc *SpannerClient) GetNodeEdgesByID(ctx context.Context, ids []string, arc *v2.Arc) (map[string][]*Edge, error) {
	// TODO: Support additional Node functionality (properties, pagination, etc).
	edges := make(map[string][]*Edge)
	if len(ids) == 0 {
		return edges, nil
	}

	var stmt spanner.Statement

	switch arc.Out {
	case true:
		stmt = spanner.Statement{
			SQL:    statements.getEdgesBySubjectID,
			Params: map[string]interface{}{"ids": ids},
		}
	case false:
		stmt = spanner.Statement{
			SQL:    statements.getEdgesByObjectID,
			Params: map[string]interface{}{"ids": ids},
		}
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
