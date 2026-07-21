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
	"strings"
)

// MultiEntityStatements contains preformatted SQL templates for multi-entity query builders.
type MultiEntityStatements struct {
	getObsBoth                                     string
	getObsBothWithDate                             string
	getObsBothLatest                               string
	getObsEntitiesOnly                             string
	getObsEntitiesOnlyWithDate                     string
	getObsEntitiesOnlyLatest                       string
	getObsByContainedInPlaceTypeFirst              containedInPlaceAccessPathStatements
	getObsByContainedInPlaceAncestorFirst          containedInPlaceAccessPathStatements
	getSdmxObs                                     string
	getSdmxAvailability                            string
	getSdmxContainedInPlace                        string
	sdmxContainedPlacesCTE                         string
	sdmxContainedPlacesWithRemoteCTE               string
	sdmxContainedSeriesCTE                         string
	getStatVarsByEntityBoth                        string
	getStatVarsByEntityVarsOnly                    string
	getStatVarsByEntityEntitiesOnly                string
	checkGroupPlaceExistence                       string
	getStatVarGroupNode                            string
	getStatVarGroupNodeWithDefinitions             string
	filterDescendentStatVarsByTimeSeries           string
	selectDescendentStatVarsFromTimeSeries         string
	selectDescendentStatVarsFromEntitySlots        string
	joinDescendentStatVarsByProvenance             string
	filterDescendentStatVarsByProvenancePredicate  string
	filterDescendentStatVarsByProvenanceObject     string
	filterDescendentStatVarsByNumEntitiesExistence string
	filterEntity1ByPlaces                          string
	filterEntity2ByPlaces                          string
	filterEntity3ByPlaces                          string
	filterEntity2Exists                            string
	filterEntity3Exists                            string
}

type containedInPlaceStatements struct {
	all      string
	withDate string
	latest   string
}

type containedInPlaceAccessPathStatements struct {
	variableSeek containedInPlaceStatements
	entityScan   containedInPlaceStatements
}

// NewMultiEntityStatements builds multi-entity SQL templates from table configuration.
func NewMultiEntityStatements(cfg TableConfig) (*MultiEntityStatements, error) {
	if err := validateMultiEntityTableConfig(cfg); err != nil {
		return nil, err
	}
	typeFirstContainedInPlace := newContainedInPlaceAccessPathStatements(cfg, typeFirstPlacesCTE)
	ancestorFirstContainedInPlace := newContainedInPlaceAccessPathStatements(cfg, ancestorFirstPlacesCTE)

	return &MultiEntityStatements{
		// Retrieve observations where both variables and entities are present (full series)
		getObsBoth: fmt.Sprintf(`		WITH params AS (
			SELECT var, ent
			FROM UNNEST(@variables) AS var
			CROSS JOIN UNNEST(@entities) AS ent
		)
		SELECT
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facet_id,
			t.provenance,
			COALESCE(
				(
					SELECT ARRAY_AGG(STRUCT(date, value AS str_value))
					FROM %[1]s o
					WHERE o.variable_measured = t.variable_measured
						AND o.entity1 = t.entity1
						AND o.extra_entities_id = t.extra_entities_id
						AND o.facet_id = t.facet_id
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facet AS facets
		FROM params p
		JOIN@{JOIN_METHOD=APPLY_JOIN} %[2]s t
			ON t.variable_measured = p.var AND t.entity1 = p.ent`, cfg.ObservationTable, cfg.TimeSeriesTable),

		// Retrieve observations for a specific date (both variables and entities present)
		getObsBothWithDate: fmt.Sprintf(`		@{SCAN_METHOD=COLUMNAR, EXECUTION_METHOD=BATCH}
		WITH params AS (
			SELECT var, ent
			FROM UNNEST(@variables) AS var
			CROSS JOIN UNNEST(@entities) AS ent
		),
		series AS (
			SELECT
				t.variable_measured,
				t.entity1,
				t.extra_entities_id,
				t.facet_id,
				t.provenance,
				t.facet
			FROM params p
			JOIN@{JOIN_METHOD=APPLY_JOIN} %[2]s t
				ON t.variable_measured = p.var AND t.entity1 = p.ent
		)
		SELECT
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facet_id,
			t.provenance,
			ARRAY(
				SELECT AS STRUCT
					o.date AS date,
					o.value AS str_value
			) AS dates_and_values,
			t.facet AS facets
		FROM series t
		JOIN@{JOIN_METHOD=APPLY_JOIN} %[1]s o
		USING (variable_measured, entity1, extra_entities_id, facet_id)
		WHERE o.date = @date`, cfg.ObservationTable, cfg.TimeSeriesTable),

		// Retrieve latest observation (both variables and entities present)
		getObsBothLatest: fmt.Sprintf(`		WITH params AS (
			SELECT var, ent
			FROM UNNEST(@variables) AS var
			CROSS JOIN UNNEST(@entities) AS ent
		)
		SELECT
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facet_id,
			t.provenance,
			COALESCE(
				(
					SELECT ARRAY(
						SELECT AS STRUCT date, value AS str_value
						FROM %[1]s o
						WHERE o.variable_measured = t.variable_measured
							AND o.entity1 = t.entity1
							AND o.extra_entities_id = t.extra_entities_id
							AND o.facet_id = t.facet_id
						ORDER BY date DESC
						LIMIT 1
					)
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facet AS facets
		FROM params p
		JOIN@{JOIN_METHOD=APPLY_JOIN} %[2]s t
			ON t.variable_measured = p.var AND t.entity1 = p.ent`, cfg.ObservationTable, cfg.TimeSeriesTable),

		// Retrieve observations where only entities are present (fetch all variables, full series)
		getObsEntitiesOnly: fmt.Sprintf(`		SELECT
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facet_id,
			t.provenance,
			COALESCE(
				(
					SELECT ARRAY_AGG(STRUCT(date, value AS str_value))
					FROM %[1]s o
					WHERE o.variable_measured = t.variable_measured
						AND o.entity1 = t.entity1
						AND o.extra_entities_id = t.extra_entities_id
						AND o.facet_id = t.facet_id
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facet AS facets
		FROM %[2]s t
		WHERE t.entity1 IN UNNEST(@entities)`, cfg.ObservationTable, cfg.TimeSeriesTable),

		// Retrieve observations where only entities are present (fetch all variables, specific date)
		getObsEntitiesOnlyWithDate: fmt.Sprintf(`		@{SCAN_METHOD=COLUMNAR, EXECUTION_METHOD=BATCH}
		WITH series AS (
			SELECT
				t.variable_measured,
				t.entity1,
				t.extra_entities_id,
				t.facet_id,
				t.provenance,
				t.facet
			FROM %[2]s t
			WHERE t.entity1 IN UNNEST(@entities)
		)
		SELECT
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facet_id,
			t.provenance,
			ARRAY(
				SELECT AS STRUCT
					o.date AS date,
					o.value AS str_value
			) AS dates_and_values,
			t.facet AS facets
		FROM series t
		JOIN@{JOIN_METHOD=APPLY_JOIN} %[1]s o
		USING (variable_measured, entity1, extra_entities_id, facet_id)
		WHERE o.date = @date`, cfg.ObservationTable, cfg.TimeSeriesTable),

		// Retrieve observations where only entities are present (fetch all variables, latest only)
		getObsEntitiesOnlyLatest: fmt.Sprintf(`		SELECT
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facet_id,
			t.provenance,
			COALESCE(
				(
					SELECT ARRAY(
						SELECT AS STRUCT date, value AS str_value
						FROM %[1]s o
						WHERE o.variable_measured = t.variable_measured
							AND o.entity1 = t.entity1
							AND o.extra_entities_id = t.extra_entities_id
							AND o.facet_id = t.facet_id
						ORDER BY date DESC
						LIMIT 1
					)
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facet AS facets
		FROM %[2]s t
		WHERE t.entity1 IN UNNEST(@entities)`, cfg.ObservationTable, cfg.TimeSeriesTable),

		getObsByContainedInPlaceTypeFirst:     typeFirstContainedInPlace,
		getObsByContainedInPlaceAncestorFirst: ancestorFirstContainedInPlace,
		getSdmxObs: fmt.Sprintf("\t\tSELECT \n"+`			t.variable_measured,
			t.entity1 AS observation_about,
			t.facet_id,
			t.provenance,
			COALESCE(
				(
					SELECT ARRAY_AGG(STRUCT(date, value AS str_value))
					FROM %[1]s o
					WHERE o.variable_measured = t.variable_measured
						AND o.entity1 = t.entity1
						AND o.extra_entities_id = t.extra_entities_id
						AND o.facet_id = t.facet_id
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facet AS facets,
			t.entities
		FROM %[2]s t
		WHERE `, cfg.ObservationTable, cfg.TimeSeriesTable),

		getSdmxAvailability: fmt.Sprintf(`		SELECT DISTINCT %%[1]s AS value
		FROM %[1]s t
		WHERE (%%[2]s)
			AND %%[1]s IS NOT NULL
			AND %%[1]s != ''
		ORDER BY value`, cfg.TimeSeriesTable) + "\n",

		// Force typeOf edges as the left input so Spanner filters by place type
		// before containment. A broad containment lookup can return every place
		// under an ancestor and usually produces a larger intermediate result.
		sdmxContainedPlacesCTE: `%[1]s AS (
			SELECT DISTINCT contained.subject_id AS place_id
			FROM Edge typed
			JOIN@{FORCE_JOIN_ORDER=TRUE} Edge contained ON contained.subject_id = typed.subject_id
			WHERE contained.predicate = '%[2]s'
				AND contained.object_id = @%[3]s
				AND typed.predicate = '%[4]s'
				AND typed.object_id = @%[5]s
		)`,

		sdmxContainedPlacesWithRemoteCTE: `%[1]s AS (
			SELECT DISTINCT contained.subject_id AS place_id
			FROM Edge typed
			JOIN@{FORCE_JOIN_ORDER=TRUE} Edge contained ON contained.subject_id = typed.subject_id
			WHERE contained.predicate = '%[2]s'
				AND contained.object_id = @%[3]s
				AND typed.predicate = '%[4]s'
				AND typed.object_id = @%[5]s
			UNION DISTINCT
			SELECT place_id
			FROM UNNEST(@%[6]s) AS place_id
		)`,

		sdmxContainedSeriesCTE: fmt.Sprintf(`series AS (
			SELECT
				t.variable_measured,
				t.entity1,
				t.extra_entities_id,
				t.facet_id,
				t.provenance,
				t.facet,
				t.entities
			FROM %%[1]s anchor
			JOIN@{JOIN_METHOD=APPLY_JOIN} %s@{FORCE_INDEX=%%[2]s} t
				ON t.%%[3]s = anchor.place_id
				AND %%[4]s%%[5]s
		)`, cfg.TimeSeriesTable),

		getSdmxContainedInPlace: fmt.Sprintf(`		%%[1]sWITH %%[2]s,
		%%[3]s
		SELECT
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facet_id,
			ANY_VALUE(t.provenance) AS provenance,
			ARRAY_AGG(STRUCT(o.date AS date, o.value AS str_value)) AS dates_and_values,
			ANY_VALUE(t.facet) AS facets,
			ANY_VALUE(t.entities) AS entities
		FROM series t
		JOIN@{JOIN_METHOD=APPLY_JOIN} %s o
		USING (variable_measured, entity1, extra_entities_id, facet_id)
		GROUP BY
			t.variable_measured,
			t.entity1,
			t.extra_entities_id,
			t.facet_id`, cfg.ObservationTable),

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
		SELECT variable_measured, entity FROM slot3`, cfg.TimeSeriesTable, cfg.TimeSeriesByEntity2Index, cfg.TimeSeriesByEntity3Index),

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
		SELECT variable_measured, entity FROM slot3`, cfg.TimeSeriesTable, cfg.TimeSeriesByEntity2Index, cfg.TimeSeriesByEntity3Index),

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
		SELECT variable_measured, entity FROM slot3`, cfg.TimeSeriesTable, cfg.TimeSeriesByEntity2Index, cfg.TimeSeriesByEntity3Index),

		checkGroupPlaceExistence: fmt.Sprintf(`		WITH
		group_members AS (
			SELECT DISTINCT
				e.object_id AS variable_group,
				e.subject_id AS variable_measured
			FROM Edge@{FORCE_INDEX=InEdge} e
			WHERE e.predicate = @predicate
			  AND e.object_id IN UNNEST(@variableGroups)
		),
		slot1 AS (
			SELECT DISTINCT
				gm.variable_group AS variable,
				ts.entity1 AS entity
			FROM group_members gm
			JOIN %[1]s AS ts
				ON gm.variable_measured = ts.variable_measured
			WHERE ts.entity1 IN UNNEST(@entities)
		),
		slot2 AS (
			SELECT DISTINCT
				gm.variable_group AS variable,
				ts.entity2 AS entity
			FROM group_members gm
			JOIN %[1]s@{FORCE_INDEX=%[2]s} AS ts
				ON gm.variable_measured = ts.variable_measured
			WHERE ts.entity2 IN UNNEST(@entities)
			  AND ts.entity2 IS NOT NULL
		),
		slot3 AS (
			SELECT DISTINCT
				gm.variable_group AS variable,
				ts.entity3 AS entity
			FROM group_members gm
			JOIN %[1]s@{FORCE_INDEX=%[3]s} AS ts
				ON gm.variable_measured = ts.variable_measured
			WHERE ts.entity3 IN UNNEST(@entities)
			  AND ts.entity3 IS NOT NULL
			  AND ts.entity2 IS NOT NULL
		)
		SELECT DISTINCT variable, entity
		FROM (
			SELECT variable, entity FROM slot1
			UNION ALL
			SELECT variable, entity FROM slot2
			UNION ALL
			SELECT variable, entity FROM slot3
		) AS combined
		ORDER BY variable, entity`, cfg.TimeSeriesTable, cfg.TimeSeriesByEntity2Index, cfg.TimeSeriesByEntity3Index),

		getStatVarGroupNode: fmt.Sprintf(`		WITH ChildSVGs AS (
				SELECT DISTINCT
					subject_id AS child_svg,
					object_id AS svg
				FROM Edge
				WHERE predicate = 'specializationOf'
				AND object_id %%[1]s
				UNION ALL
				%%[2]s
			),
			UniqueChildSVGs AS (
				SELECT DISTINCT child_svg FROM ChildSVGs
			),
			ChildSVGCounts AS (
				SELECT
					e.object_id AS child_svg,
					COUNT(e.subject_id) AS descendent_stat_var_count
				FROM UniqueChildSVGs u
				JOIN@{JOIN_METHOD=APPLY_JOIN} Edge e
				ON e.object_id = u.child_svg
				WHERE e.predicate = 'linkedMemberOf'
				GROUP BY e.object_id
			),
			ChildSVs AS (
				SELECT DISTINCT
					subject_id AS child_sv,
					object_id AS svg
				FROM Edge
				WHERE predicate = 'memberOf'
				AND object_id %%[1]s
			),
			UniqueChildSVs AS (
				SELECT DISTINCT child_sv FROM ChildSVs
			)
			SELECT
				svg.svg,
				n.subject_id,
				IFNULL(n.name, '') AS name,
				c.descendent_stat_var_count,
				FALSE AS has_data,
				'' AS definition
			FROM ChildSVGs svg
			JOIN ChildSVGCounts c
			ON svg.child_svg = c.child_svg
			JOIN Node n
			ON n.subject_id = svg.child_svg
			UNION ALL
			SELECT
				sv.svg,
				n.subject_id,
				IFNULL(n.name, '') AS name,
				-1 AS descendent_stat_var_count,
				EXISTS (
					SELECT 1
					FROM %[1]s ts
					WHERE ts.variable_measured = sv.child_sv
					LIMIT 1
				) AS has_data,
				'' AS definition
			FROM ChildSVs sv
			JOIN Node n
			ON n.subject_id = sv.child_sv`, cfg.TimeSeriesTable),

		getStatVarGroupNodeWithDefinitions: fmt.Sprintf(`		WITH ChildSVGs AS (
				SELECT DISTINCT
					subject_id AS child_svg,
					object_id AS svg
				FROM Edge
				WHERE predicate = 'specializationOf'
				AND object_id %%[1]s
				UNION ALL
				%%[2]s
			),
			UniqueChildSVGs AS (
				SELECT DISTINCT child_svg FROM ChildSVGs
			),
			ChildSVGCounts AS (
				SELECT
					e.object_id AS child_svg,
					COUNT(e.subject_id) AS descendent_stat_var_count
				FROM UniqueChildSVGs u
				JOIN@{JOIN_METHOD=APPLY_JOIN} Edge e
				ON e.object_id = u.child_svg
				WHERE e.predicate = 'linkedMemberOf'
				GROUP BY e.object_id
			),
			ChildSVs AS (
				SELECT DISTINCT
					subject_id AS child_sv,
					object_id AS svg
				FROM Edge
				WHERE predicate = 'memberOf'
				AND object_id %%[1]s
			),
			UniqueChildSVs AS (
				SELECT DISTINCT child_sv FROM ChildSVs
			)
			SELECT
				svg.svg,
				n.subject_id,
				IFNULL(n.name, '') AS name,
				c.descendent_stat_var_count,
				FALSE AS has_data,
				'' AS definition
			FROM ChildSVGs svg
			JOIN ChildSVGCounts c
			ON svg.child_svg = c.child_svg
			JOIN Node n
			ON n.subject_id = svg.child_svg
			UNION ALL
			SELECT
				sv.svg,
				n.subject_id,
				IFNULL(n.name, '') AS name,
				-1 AS descendent_stat_var_count,
				EXISTS (
					SELECT 1
					FROM %[1]s ts
					WHERE ts.variable_measured = sv.child_sv
					LIMIT 1
				) AS has_data,
				IFNULL((
					SELECT n_def.value
					FROM Edge e_def
					JOIN Node n_def ON e_def.object_id = n_def.subject_id
					WHERE e_def.subject_id = sv.child_sv
					AND e_def.predicate = 'definition'
					LIMIT 1
				), '') AS definition
			FROM ChildSVs sv
			JOIN Node n
			ON n.subject_id = sv.child_sv`, cfg.TimeSeriesTable),

		filterDescendentStatVarsByTimeSeries: `JOIN@{JOIN_TYPE=HASH_JOIN} (
					SELECT ts.variable_measured
					FROM %s%s%s
					GROUP BY ts.variable_measured%s
				) o ON o.variable_measured = e.subject_id`,

		selectDescendentStatVarsFromTimeSeries: fmt.Sprintf(`%s AS ts`, cfg.TimeSeriesTable),

		selectDescendentStatVarsFromEntitySlots: fmt.Sprintf(`(
						SELECT t.variable_measured, t.entity1 AS entity, t.provenance
						FROM %[1]s AS t
						%%[1]s
						UNION ALL
						SELECT t.variable_measured, t.entity2 AS entity, t.provenance
						FROM %[1]s@{FORCE_INDEX=%[2]s} AS t
						%%[2]s
						UNION ALL
						SELECT t.variable_measured, t.entity3 AS entity, t.provenance
						FROM %[1]s@{FORCE_INDEX=%[3]s} AS t
						%%[3]s
					) AS ts`, cfg.TimeSeriesTable, cfg.TimeSeriesByEntity2Index, cfg.TimeSeriesByEntity3Index),

		joinDescendentStatVarsByProvenance: `
					JOIN Edge@{FORCE_INDEX=InEdge} e1
					ON ts.provenance = e1.subject_id`,

		filterDescendentStatVarsByProvenancePredicate: "e1.predicate = @predicate",

		filterDescendentStatVarsByProvenanceObject: "e1.object_id = @provenance",

		filterDescendentStatVarsByNumEntitiesExistence: `
					HAVING COUNT(DISTINCT %s) >= @numEntitiesExistence`,

		filterEntity1ByPlaces: "WHERE t.entity1 IN UNNEST(@places)",

		filterEntity2ByPlaces: `WHERE t.entity2 IN UNNEST(@places)
							AND t.entity2 IS NOT NULL`,

		filterEntity3ByPlaces: `WHERE t.entity3 IN UNNEST(@places)
							AND t.entity3 IS NOT NULL
							AND t.entity2 IS NOT NULL`,

		filterEntity2Exists: "WHERE t.entity2 IS NOT NULL",

		filterEntity3Exists: "WHERE t.entity3 IS NOT NULL\n\t\t\t\t\t\tAND t.entity2 IS NOT NULL",
	}, nil
}

const typeFirstPlacesCTE = `places AS (
			SELECT DISTINCT contained.subject_id AS place_id
			FROM Edge typed
			JOIN@{FORCE_JOIN_ORDER=TRUE} Edge contained ON contained.subject_id = typed.subject_id
			WHERE typed.predicate = 'typeOf'
				AND typed.object_id = @childPlaceType
				AND contained.predicate = 'linkedContainedInPlace'
				AND contained.object_id = @ancestor
		)`

const ancestorFirstPlacesCTE = `places AS (
			SELECT DISTINCT contained.subject_id AS place_id
			FROM Edge contained
			JOIN@{FORCE_JOIN_ORDER=TRUE} Edge typed ON typed.subject_id = contained.subject_id
			WHERE contained.predicate = 'linkedContainedInPlace'
				AND contained.object_id = @ancestor
				AND typed.predicate = 'typeOf'
				AND typed.object_id = @childPlaceType
		)`

func newContainedInPlaceAccessPathStatements(cfg TableConfig, placesCTE string) containedInPlaceAccessPathStatements {
	return containedInPlaceAccessPathStatements{
		variableSeek: newContainedInPlaceStatements(
			cfg,
			placesCTE,
			fmt.Sprintf("%s@{FORCE_INDEX=_BASE_TABLE}", cfg.TimeSeriesTable),
		),
		// Limit the seekable index prefix to entity1 so variable_measured is
		// evaluated as a residual filter instead of producing one sparse range
		// lookup for every place-variable pair.
		entityScan: newContainedInPlaceStatements(
			cfg,
			placesCTE,
			fmt.Sprintf("%s@{FORCE_INDEX=%s, SEEKABLE_KEY_SIZE=1}", cfg.TimeSeriesTable, cfg.TimeSeriesByEntity1Index),
		),
	}
}

func newContainedInPlaceStatements(cfg TableConfig, placesCTE, timeSeriesSource string) containedInPlaceStatements {
	return containedInPlaceStatements{
		// Join all dates before aggregation to optimize total result throughput.
		all: fmt.Sprintf(`		@{SCAN_METHOD=COLUMNAR, EXECUTION_METHOD=BATCH}
		WITH %[3]s,
		series AS (
			SELECT
				t.variable_measured,
				t.entity1,
				t.extra_entities_id,
				t.facet_id,
				t.provenance,
				t.facet
			FROM places p
			JOIN@{JOIN_METHOD=APPLY_JOIN} %[2]s t
				ON t.variable_measured IN UNNEST(@variables)
				AND t.entity1 = p.place_id
		)
		SELECT
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facet_id,
			ANY_VALUE(t.provenance) AS provenance,
			ARRAY_AGG(
				STRUCT(
					o.date AS date,
					o.value AS str_value
				)
			) AS dates_and_values,
			ANY_VALUE(t.facet) AS facets
		FROM series t
		JOIN@{JOIN_METHOD=APPLY_JOIN} %[1]s o
		USING (variable_measured, entity1, extra_entities_id, facet_id)
		GROUP BY
			t.variable_measured,
			t.entity1,
			t.extra_entities_id,
			t.facet_id`, cfg.ObservationTable, timeSeriesSource, placesCTE),

		// Join to Observation so TimeSeries without the requested date are
		// discarded in Spanner.
		withDate: fmt.Sprintf(`		@{SCAN_METHOD=COLUMNAR, EXECUTION_METHOD=BATCH}
		WITH %[3]s,
		series AS (
			SELECT
				t.variable_measured,
				t.entity1,
				t.extra_entities_id,
				t.facet_id,
				t.provenance,
				t.facet
			FROM places p
			JOIN@{JOIN_METHOD=APPLY_JOIN} %[2]s t
				ON t.variable_measured IN UNNEST(@variables)
				AND t.entity1 = p.place_id
		)
		SELECT
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facet_id,
			t.provenance,
			ARRAY(
				SELECT AS STRUCT
					o.date AS date,
					o.value AS str_value
			) AS dates_and_values,
			t.facet AS facets
		FROM series t
		JOIN@{JOIN_METHOD=APPLY_JOIN} %[1]s o
		USING (variable_measured, entity1, extra_entities_id, facet_id)
		WHERE o.date = @date`, cfg.ObservationTable, timeSeriesSource, placesCTE),

		// Use a correlated full-key lookup so the date-descending child scan can
		// stop after one row.
		latest: fmt.Sprintf(`		@{SCAN_METHOD=COLUMNAR, EXECUTION_METHOD=BATCH}
		WITH %[3]s,
		series AS (
			SELECT
				t.variable_measured,
				t.entity1,
				t.extra_entities_id,
				t.facet_id,
				t.provenance,
				t.facet
			FROM places p
			JOIN@{JOIN_METHOD=APPLY_JOIN} %[2]s t
				ON t.variable_measured IN UNNEST(@variables)
				AND t.entity1 = p.place_id
		)
		SELECT
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facet_id,
			t.provenance,
			COALESCE(
				(
					SELECT ARRAY(
						SELECT AS STRUCT
							o.date AS date,
							o.value AS str_value
						FROM %[1]s o
						WHERE o.variable_measured = t.variable_measured
							AND o.entity1 = t.entity1
							AND o.extra_entities_id = t.extra_entities_id
							AND o.facet_id = t.facet_id
						ORDER BY o.date DESC
						LIMIT 1
					)
				),
				ARRAY(
					SELECT AS STRUCT
						CAST(NULL AS STRING) AS date,
						CAST(NULL AS STRING) AS str_value
					FROM UNNEST([1])
					WHERE FALSE
				)
			) AS dates_and_values,
			t.facet AS facets
		FROM series t`, cfg.ObservationTable, timeSeriesSource, placesCTE),
	}
}

func validateMultiEntityTableConfig(cfg TableConfig) error {
	missing := []string{}
	if strings.TrimSpace(cfg.TimeSeriesTable) == "" {
		missing = append(missing, "TimeSeriesTable")
	}
	if strings.TrimSpace(cfg.ObservationTable) == "" {
		missing = append(missing, "ObservationTable")
	}
	if strings.TrimSpace(cfg.TimeSeriesByEntity1Index) == "" {
		missing = append(missing, "TimeSeriesByEntity1Index")
	}
	if strings.TrimSpace(cfg.TimeSeriesByEntity2Index) == "" {
		missing = append(missing, "TimeSeriesByEntity2Index")
	}
	if strings.TrimSpace(cfg.TimeSeriesByEntity3Index) == "" {
		missing = append(missing, "TimeSeriesByEntity3Index")
	}
	if strings.TrimSpace(cfg.TimeSeriesByProvenanceIndex) == "" {
		missing = append(missing, "TimeSeriesByProvenanceIndex")
	}
	if len(missing) > 0 {
		return fmt.Errorf("NewMultiEntityStatements: missing required TableConfig fields: %s", strings.Join(missing, ", "))
	}
	return nil
}
