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
	"strings"

	"cloud.google.com/go/spanner"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/server/v2/shared"
	"google.golang.org/api/iterator"
)

var (
	ObsColumns = []string{
		"variable_measured",
		"observation_about",
		"observation_date",
		"value",
		"provenance",
		"observation_period",
		"measurement_method",
		"unit",
		"scaling_factor",
	}
)

// SQL / GQL statements executed by the SpannerClient
var statements = struct {
	getEdgesBySubjectID             string
	getEdgesByObjectID              string
	getObsByVariableAndEntity       string
	getObsByVariableEntityAndDate   string
	getLatestObsByVariableAndEntity string
}{
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
	getObsByVariableAndEntity: fmt.Sprintf(`
		SELECT %s
		FROM StatVarObservation
		WHERE
			variable_measured IN UNNEST(@variables) AND
			observation_about IN UNNEST(@entities) AND
			value != ''
		ORDER BY observation_date ASC
	`,
		getSelectColumns(ObsColumns, "")),
	getObsByVariableEntityAndDate: fmt.Sprintf(`
		SELECT %s
		FROM StatVarObservation
		WHERE
			variable_measured IN UNNEST(@variables) AND
			observation_about IN UNNEST(@entities) AND
			observation_date = @date AND
			value != ''
		ORDER BY observation_date ASC
	`,
		getSelectColumns(ObsColumns, "")),
	getLatestObsByVariableAndEntity: fmt.Sprintf(`
		SELECT %s
		FROM StatVarObservation AS t1
		INNER JOIN (
			SELECT
				variable_measured,
				observation_about,
				provenance,
				observation_period,
				measurement_method,
				unit,
				scaling_factor,
				MAX(observation_date) AS max_observation_date
				FROM
				StatVarObservation
				WHERE variable_measured IN UNNEST(@variables)
				AND observation_about IN UNNEST(@entities)
				GROUP BY 1, 2, 3, 4, 5, 6, 7
			) AS t2 
			ON t1.variable_measured = t2.variable_measured
			AND t1.observation_about = t2.observation_about
			AND (t1.provenance = t2.provenance OR (t1.provenance IS NULL AND t2.provenance IS NULL))
			AND (t1.observation_period = t2.observation_period OR (t1.observation_period IS NULL AND t2.observation_period IS NULL))
			AND (t1.unit = t2.unit OR (t1.unit IS NULL AND t2.unit IS NULL))
			AND (t1.scaling_factor = t2.scaling_factor OR (t1.scaling_factor IS NULL AND t2.scaling_factor IS NULL))
			AND t1.observation_date = t2.max_observation_date
		WHERE t1.variable_measured IN UNNEST(@variables)
		AND t1.observation_about IN UNNEST(@entities)
	`,
		getSelectColumns(ObsColumns, "t1.")),
}

// GetNodeEdgesByID retrieves node edges from Spanner given a V3 NodeRequest and returns a map.
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

// GetObservations retrieves observations from Spanner given a list of variables, entities and date.
func (sc *SpannerClient) GetObservations(ctx context.Context, variables []string, entities []string, date string) ([]*StatVarObservation, error) {
	var observations []*StatVarObservation
	if len(variables) == 0 || len(entities) == 0 {
		return observations, nil
	}

	var stmt spanner.Statement

	switch date {
	case "":
		stmt = spanner.Statement{
			SQL: statements.getObsByVariableAndEntity,
			Params: map[string]interface{}{
				"variables": variables,
				"entities":  entities,
			},
		}
	case shared.LATEST:
		stmt = spanner.Statement{
			SQL: statements.getLatestObsByVariableAndEntity,
			Params: map[string]interface{}{
				"variables": variables,
				"entities":  entities,
			},
		}
	default:
		stmt = spanner.Statement{
			SQL: statements.getObsByVariableEntityAndDate,
			Params: map[string]interface{}{
				"variables": variables,
				"entities":  entities,
				"date":      date,
			},
		}
	}

	err := sc.queryAndCollect(
		ctx,
		stmt,
		func() interface{} {
			return &StatVarObservation{}
		},
		func(rowStruct interface{}) {
			observation := rowStruct.(*StatVarObservation)
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

// getSelectColumns generates the select clause from the specified columns.
// The columns are coalesced to avoid nulls.
// They are optionally prefixed if a prefix is specified (relevant from queries with joins).
func getSelectColumns(columns []string, prefix string) string {
	var prefixedCols []string
	for _, col := range columns {
		prefixedCols = append(
			prefixedCols,
			fmt.Sprintf("COALESCE(%s%s, '') AS %s", prefix, col, col))
	}
	return strings.Join(prefixedCols, ",\n")
}
