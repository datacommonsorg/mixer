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

import "fmt"

const (
	timeSeriesTable  = "TimeSeries_final_v2"
	observationTable = "Observation_final_v2"

	timeSeriesByEntity2Index = "TimeSeriesFinalV2ByEntity2"
	timeSeriesByEntity3Index = "TimeSeriesFinalV2ByEntity3"
)

var statementsMultiEntity = struct {
	getObsBoth                                   string
	getObsBothWithDate                           string
	getObsBothLatest                             string
	getObsEntitiesOnly                           string
	getObsEntitiesOnlyWithDate                   string
	getObsEntitiesOnlyLatest                     string
	getObsByContainedInPlaceBoth                 string
	getObsByContainedInPlaceBothWithDate         string
	getObsByContainedInPlaceBothLatest           string
	getObsByContainedInPlaceEntitiesOnly         string
	getObsByContainedInPlaceEntitiesOnlyWithDate string
	getObsByContainedInPlaceEntitiesOnlyLatest   string
	getStatVarsByEntityBoth                      string
	getStatVarsByEntityVarsOnly                  string
	getStatVarsByEntityEntitiesOnly              string
}{
	// Retrieve observations where both variables and entities are present (full series)
	getObsBoth: fmt.Sprintf(`		WITH params AS (
			SELECT var, ent 
			FROM UNNEST(@variables) AS var 
			CROSS JOIN UNNEST(@entities) AS ent
		)
		SELECT 
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facets_id AS facet_id,
			COALESCE(
				(
					SELECT ARRAY_AGG(STRUCT(date, str_value)) 
					FROM %s o
					WHERE o.variable_measured = t.variable_measured
						AND o.entity1 = t.entity1
						AND o.extra_entities_id = t.extra_entities_id
						AND o.facets_id = t.facets_id
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facets
		FROM params p
		JOIN@{JOIN_METHOD=APPLY_JOIN, FORCE_JOIN_ORDER=TRUE} %s t
			ON t.variable_measured = p.var AND t.entity1 = p.ent`, observationTable, timeSeriesTable),

	// Retrieve observations for a specific date (both variables and entities present)
	getObsBothWithDate: fmt.Sprintf(`		WITH params AS (
			SELECT var, ent 
			FROM UNNEST(@variables) AS var 
			CROSS JOIN UNNEST(@entities) AS ent
		)
		SELECT 
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facets_id AS facet_id,
			COALESCE(
				(
					SELECT ARRAY_AGG(STRUCT(date, str_value)) 
					FROM %s o
					WHERE o.variable_measured = t.variable_measured
						AND o.entity1 = t.entity1
						AND o.extra_entities_id = t.extra_entities_id
						AND o.facets_id = t.facets_id
						AND o.date = @date
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facets
		FROM params p
		JOIN@{JOIN_METHOD=APPLY_JOIN, FORCE_JOIN_ORDER=TRUE} %s t
			ON t.variable_measured = p.var AND t.entity1 = p.ent`, observationTable, timeSeriesTable),

	// Retrieve latest observation (both variables and entities present)
	getObsBothLatest: fmt.Sprintf(`		WITH params AS (
			SELECT var, ent 
			FROM UNNEST(@variables) AS var 
			CROSS JOIN UNNEST(@entities) AS ent
		)
		SELECT 
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facets_id AS facet_id,
			COALESCE(
				(
					SELECT ARRAY(
						SELECT AS STRUCT date, str_value
						FROM %s o
						WHERE o.variable_measured = t.variable_measured
							AND o.entity1 = t.entity1
							AND o.extra_entities_id = t.extra_entities_id
							AND o.facets_id = t.facets_id
						ORDER BY date DESC
						LIMIT 1
					)
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facets
		FROM params p
		JOIN@{JOIN_METHOD=APPLY_JOIN, FORCE_JOIN_ORDER=TRUE} %s t
			ON t.variable_measured = p.var AND t.entity1 = p.ent`, observationTable, timeSeriesTable),

	// Retrieve observations where only entities are present (fetch all variables, full series)
	getObsEntitiesOnly: fmt.Sprintf(`		SELECT 
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facets_id AS facet_id,
			COALESCE(
				(
					SELECT ARRAY_AGG(STRUCT(date, str_value)) 
					FROM %s o
					WHERE o.variable_measured = t.variable_measured
						AND o.entity1 = t.entity1
						AND o.extra_entities_id = t.extra_entities_id
						AND o.facets_id = t.facets_id
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facets
		FROM %s t
		WHERE t.entity1 IN UNNEST(@entities)`, observationTable, timeSeriesTable),

	// Retrieve observations where only entities are present (fetch all variables, specific date)
	getObsEntitiesOnlyWithDate: fmt.Sprintf(`		SELECT 
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facets_id AS facet_id,
			COALESCE(
				(
					SELECT ARRAY_AGG(STRUCT(date, str_value)) 
					FROM %s o
					WHERE o.variable_measured = t.variable_measured
						AND o.entity1 = t.entity1
						AND o.extra_entities_id = t.extra_entities_id
						AND o.facets_id = t.facets_id
						AND o.date = @date
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facets
		FROM %s t
		WHERE t.entity1 IN UNNEST(@entities)`, observationTable, timeSeriesTable),

	// Retrieve observations where only entities are present (fetch all variables, latest only)
	getObsEntitiesOnlyLatest: fmt.Sprintf(`		SELECT 
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facets_id AS facet_id,
			COALESCE(
				(
					SELECT ARRAY(
						SELECT AS STRUCT date, str_value
						FROM %s o
						WHERE o.variable_measured = t.variable_measured
							AND o.entity1 = t.entity1
							AND o.extra_entities_id = t.extra_entities_id
							AND o.facets_id = t.facets_id
						ORDER BY date DESC
						LIMIT 1
					)
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facets
		FROM %s t
		WHERE t.entity1 IN UNNEST(@entities)`, observationTable, timeSeriesTable),

	// Contained in place query with variables filtered (full series)
	getObsByContainedInPlaceBoth: fmt.Sprintf(`		WITH places AS (
			SELECT result.object_id AS place_id
			FROM GRAPH_TABLE ( 
				DCGraph MATCH <-[e:Edge WHERE e.object_id = @ancestor AND e.predicate = 'linkedContainedInPlace']-()-[{predicate: 'typeOf', object_id: @childPlaceType}]-> 
				RETURN e.subject_id AS object_id 
			) result
		)
		SELECT 
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facets_id AS facet_id,
			COALESCE(
				(
					SELECT ARRAY_AGG(STRUCT(date, str_value)) 
					FROM %s o
					WHERE o.variable_measured = t.variable_measured
						AND o.entity1 = t.entity1
						AND o.extra_entities_id = t.extra_entities_id
						AND o.facets_id = t.facets_id
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facets
		FROM places p
		JOIN@{JOIN_METHOD=APPLY_JOIN, FORCE_JOIN_ORDER=TRUE} %s t
			ON t.variable_measured IN UNNEST(@variables)
			AND t.entity1 = p.place_id`, observationTable, timeSeriesTable),

	// Contained in place query with variables filtered (specific date)
	getObsByContainedInPlaceBothWithDate: fmt.Sprintf(`		WITH places AS (
			SELECT result.object_id AS place_id
			FROM GRAPH_TABLE ( 
				DCGraph MATCH <-[e:Edge WHERE e.object_id = @ancestor AND e.predicate = 'linkedContainedInPlace']-()-[{predicate: 'typeOf', object_id: @childPlaceType}]-> 
				RETURN e.subject_id AS object_id 
			) result
		)
		SELECT 
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facets_id AS facet_id,
			COALESCE(
				(
					SELECT ARRAY_AGG(STRUCT(date, str_value)) 
					FROM %s o
					WHERE o.variable_measured = t.variable_measured
						AND o.entity1 = t.entity1
						AND o.extra_entities_id = t.extra_entities_id
						AND o.facets_id = t.facets_id
						AND o.date = @date
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facets
		FROM places p
		JOIN@{JOIN_METHOD=APPLY_JOIN, FORCE_JOIN_ORDER=TRUE} %s t
			ON t.variable_measured IN UNNEST(@variables)
			AND t.entity1 = p.place_id`, observationTable, timeSeriesTable),

	// Contained in place query with variables filtered (latest only)
	getObsByContainedInPlaceBothLatest: fmt.Sprintf(`		WITH places AS (
			SELECT result.object_id AS place_id
			FROM GRAPH_TABLE ( 
				DCGraph MATCH <-[e:Edge WHERE e.object_id = @ancestor AND e.predicate = 'linkedContainedInPlace']-()-[{predicate: 'typeOf', object_id: @childPlaceType}]-> 
				RETURN e.subject_id AS object_id 
			) result
		)
		SELECT 
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facets_id AS facet_id,
			COALESCE(
				(
					SELECT ARRAY(
						SELECT AS STRUCT date, str_value
						FROM %s o
						WHERE o.variable_measured = t.variable_measured
							AND o.entity1 = t.entity1
							AND o.extra_entities_id = t.extra_entities_id
							AND o.facets_id = t.facets_id
						ORDER BY date DESC
						LIMIT 1
					)
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facets
		FROM places p
		JOIN@{JOIN_METHOD=APPLY_JOIN, FORCE_JOIN_ORDER=TRUE} %s t
			ON t.variable_measured IN UNNEST(@variables)
			AND t.entity1 = p.place_id`, observationTable, timeSeriesTable),

	// Contained in place query without variables filtered (full series)
	getObsByContainedInPlaceEntitiesOnly: fmt.Sprintf(`		WITH places AS (
			SELECT result.object_id AS place_id
			FROM GRAPH_TABLE ( 
				DCGraph MATCH <-[e:Edge WHERE e.object_id = @ancestor AND e.predicate = 'linkedContainedInPlace']-()-[{predicate: 'typeOf', object_id: @childPlaceType}]-> 
				RETURN e.subject_id AS object_id 
			) result
		)
		SELECT 
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facets_id AS facet_id,
			COALESCE(
				(
					SELECT ARRAY_AGG(STRUCT(date, str_value)) 
					FROM %s o
					WHERE o.variable_measured = t.variable_measured
						AND o.entity1 = t.entity1
						AND o.extra_entities_id = t.extra_entities_id
						AND o.facets_id = t.facets_id
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facets
		FROM places p
		JOIN@{JOIN_METHOD=APPLY_JOIN, FORCE_JOIN_ORDER=TRUE} %s t
			ON t.entity1 = p.place_id`, observationTable, timeSeriesTable),

	// Contained in place query without variables filtered (specific date)
	getObsByContainedInPlaceEntitiesOnlyWithDate: fmt.Sprintf(`		WITH places AS (
			SELECT result.object_id AS place_id
			FROM GRAPH_TABLE ( 
				DCGraph MATCH <-[e:Edge WHERE e.object_id = @ancestor AND e.predicate = 'linkedContainedInPlace']-()-[{predicate: 'typeOf', object_id: @childPlaceType}]-> 
				RETURN e.subject_id AS object_id 
			) result
		)
		SELECT 
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facets_id AS facet_id,
			COALESCE(
				(
					SELECT ARRAY_AGG(STRUCT(date, str_value)) 
					FROM %s o
					WHERE o.variable_measured = t.variable_measured
						AND o.entity1 = t.entity1
						AND o.extra_entities_id = t.extra_entities_id
						AND o.facets_id = t.facets_id
						AND o.date = @date
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facets
		FROM places p
		JOIN@{JOIN_METHOD=APPLY_JOIN, FORCE_JOIN_ORDER=TRUE} %s t
			ON t.entity1 = p.place_id`, observationTable, timeSeriesTable),

	// Contained in place query without variables filtered (latest only)
	getObsByContainedInPlaceEntitiesOnlyLatest: fmt.Sprintf(`		WITH places AS (
			SELECT result.object_id AS place_id
			FROM GRAPH_TABLE ( 
				DCGraph MATCH <-[e:Edge WHERE e.object_id = @ancestor AND e.predicate = 'linkedContainedInPlace']-()-[{predicate: 'typeOf', object_id: @childPlaceType}]-> 
				RETURN e.subject_id AS object_id 
			) result
		)
		SELECT 
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facets_id AS facet_id,
			COALESCE(
				(
					SELECT ARRAY(
						SELECT AS STRUCT date, str_value
						FROM %s o
						WHERE o.variable_measured = t.variable_measured
							AND o.entity1 = t.entity1
							AND o.extra_entities_id = t.extra_entities_id
							AND o.facets_id = t.facets_id
						ORDER BY date DESC
						LIMIT 1
					)
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facets
		FROM places p
		JOIN@{JOIN_METHOD=APPLY_JOIN, FORCE_JOIN_ORDER=TRUE} %s t
			ON t.entity1 = p.place_id`, observationTable, timeSeriesTable),

	// Check existence when both variables and entities are specified
	getStatVarsByEntityBoth: fmt.Sprintf(`		WITH 
		slot1 AS (
			SELECT DISTINCT t.variable_measured, t.entity1 AS entity 
			FROM %[1]s AS t 
			WHERE t.variable_measured IN UNNEST(@variables) AND t.entity1 IN UNNEST(@entities)
		),
		slot2 AS (
			SELECT DISTINCT t.variable_measured, t.entity2 AS entity 
			FROM %[1]s@{FORCE_INDEX=%[2]s} AS t 
			WHERE t.variable_measured IN UNNEST(@variables) AND t.entity2 IN UNNEST(@entities) AND t.entity2 IS NOT NULL
		),
		slot3 AS (
			SELECT DISTINCT t.variable_measured, t.entity3 AS entity 
			FROM %[1]s@{FORCE_INDEX=%[3]s} AS t 
			WHERE t.variable_measured IN UNNEST(@variables) AND t.entity3 IN UNNEST(@entities) AND t.entity3 IS NOT NULL AND t.entity2 IS NOT NULL
		)
		SELECT variable_measured, entity FROM slot1
		UNION ALL
		SELECT variable_measured, entity FROM slot2
		UNION ALL
		SELECT variable_measured, entity FROM slot3`, timeSeriesTable, timeSeriesByEntity2Index, timeSeriesByEntity3Index),

	// Check existence when only variables are specified
	getStatVarsByEntityVarsOnly: fmt.Sprintf(`		WITH 
		slot1 AS (
			SELECT DISTINCT t.variable_measured, t.entity1 AS entity 
			FROM %[1]s AS t 
			WHERE t.variable_measured IN UNNEST(@variables)
		),
		slot2 AS (
			SELECT DISTINCT t.variable_measured, t.entity2 AS entity 
			FROM %[1]s@{FORCE_INDEX=%[2]s} AS t 
			WHERE t.variable_measured IN UNNEST(@variables) AND t.entity2 IS NOT NULL
		),
		slot3 AS (
			SELECT DISTINCT t.variable_measured, t.entity3 AS entity 
			FROM %[1]s@{FORCE_INDEX=%[3]s} AS t 
			WHERE t.variable_measured IN UNNEST(@variables) AND t.entity3 IS NOT NULL AND t.entity2 IS NOT NULL
		)
		SELECT variable_measured, entity FROM slot1
		UNION ALL
		SELECT variable_measured, entity FROM slot2
		UNION ALL
		SELECT variable_measured, entity FROM slot3`, timeSeriesTable, timeSeriesByEntity2Index, timeSeriesByEntity3Index),

	// Check existence when only entities are specified
	getStatVarsByEntityEntitiesOnly: fmt.Sprintf(`		WITH 
		slot1 AS (
			SELECT DISTINCT t.variable_measured, t.entity1 AS entity 
			FROM %[1]s AS t 
			WHERE t.entity1 IN UNNEST(@entities)
		),
		slot2 AS (
			SELECT DISTINCT t.variable_measured, t.entity2 AS entity 
			FROM %[1]s@{FORCE_INDEX=%[2]s} AS t 
			WHERE t.entity2 IN UNNEST(@entities) AND t.entity2 IS NOT NULL
		),
		slot3 AS (
			SELECT DISTINCT t.variable_measured, t.entity3 AS entity 
			FROM %[1]s@{FORCE_INDEX=%[3]s} AS t 
			WHERE t.entity3 IN UNNEST(@entities) AND t.entity3 IS NOT NULL AND t.entity2 IS NOT NULL
		)
		SELECT variable_measured, entity FROM slot1
		UNION ALL
		SELECT variable_measured, entity FROM slot2
		UNION ALL
		SELECT variable_measured, entity FROM slot3`, timeSeriesTable, timeSeriesByEntity2Index, timeSeriesByEntity3Index),
}
