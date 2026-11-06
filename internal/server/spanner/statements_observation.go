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

// ObservationStatements contains preformatted SQL templates for the observation query builder.
type ObservationStatements struct {
	getObsBoth                                     string
	getObsBothWithDate                             string
	getObsBothLatest                               string
	getObsEntitiesOnly                             string
	getObsEntitiesOnlyWithDate                     string
	getObsEntitiesOnlyLatest                       string
	getObsByContainedInPlaceTypeFirst              containedInPlaceAccessPathStatements
	getObsByContainedInPlaceAncestorFirst          containedInPlaceAccessPathStatements
	getObsByContainedInPlaceMaterialized           containedInPlaceAccessPathStatements
	getSdmxObs                                     string
	getSdmxObsWithDates                            string
	getSdmxObsLatest                               string
	getSdmxAvailability                            string
	getSdmxAvailabilityWithDates                   string
	getSdmxAvailabilityWithDatesMergeBaseTable     string
	getSdmxContainedInPlace                        string
	getSdmxContainedInPlaceWithDates               string
	getSdmxContainedInPlaceLatest                  string
	getSdmxContainedAvailability                   string
	getSdmxContainedAvailabilityWithDates          string
	sdmxContainedPlacesEdge                        string
	sdmxContainedPlacesLinkedEdge                  string
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
	filterDescendentStatVarsByTimeSeriesBuildRight string
	selectDescendentStatVarsFromTimeSeries         string
	selectDescendentStatVarsFromEntitySlots        string
	joinDescendentStatVarsByProvenance             string
	filterDescendentStatVarsByProvenancePredicate  string
	filterDescendentStatVarsByProvenanceObject     string
	filterDescendentStatVarsByOnlyProvenance       string
	filterDescendentStatVarsByNumEntitiesExistence string
	filterEntity1ByPlaces                          string
	filterEntity2ByPlaces                          string
	filterEntity3ByPlaces                          string
	filterEntity2Exists                            string
	filterEntity3Exists                            string

	// Stat var group and topic hierarchy templates. They read only Node and
	// Edge; the builder composes them with the TimeSeries filters above.

	// Attach stat var groups.
	attachSVGs string
	// Fetch all children of a stat var group without definitions.
	getSVGChildren string
	// Fetch all children of a stat var group with definitions.
	getSVGChildrenWithDefinitions string
	// Fetch filtered count of descendent stat vars for a given variable group.
	getFilteredChildSVGs string
	// Fetch filtered descendent stat vars for a given variable group without definitions.
	getFilteredChildSVs string
	// Fetch filtered descendent stat vars for a given variable group with definitions.
	getFilteredChildSVsWithDefinitions string
	// Fetch filtered count of descendent stat vars for a given topic.
	getFilteredTopic string
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

// NewObservationStatements builds the observation SQL templates from table configuration.
func NewObservationStatements(cfg TableConfig) (*ObservationStatements, error) {
	if err := validateTableConfig(cfg); err != nil {
		return nil, err
	}
	sdmxDateStatementHint := columnarBatchStatementHint(cfg) + "\t\t"
	dateStatementHint := columnarBatchStatementHint(cfg)
	// Queries that force the NULL_FILTERED entity2/entity3 indexes always filter
	// NULL entities explicitly, but the emulator can't prove that and rejects the
	// index without its bypass hint. Production SQL is unchanged.
	entity2IndexHint := cfg.TimeSeriesByEntity2Index
	entity3IndexHint := cfg.TimeSeriesByEntity3Index
	if cfg.spannerEmulatorCompatibility {
		entity2IndexHint += ", spanner_emulator.disable_query_null_filtered_index_check=true"
		entity3IndexHint += ", spanner_emulator.disable_query_null_filtered_index_check=true"
	}
	buildSdmxAvailabilityWithDates := func(timeSeriesTable, observationTable, joinHint string) string {
		return fmt.Sprintf(`%[1]sSELECT DISTINCT %%[1]s AS value
		FROM %[2]s t
		JOIN%[4]s %[3]s o
		USING (variable_measured, entity1, extra_entities_id, facet_id)
		WHERE (%%[2]s)
			AND o.date IN UNNEST(@time_periods)
			AND %%[1]s IS NOT NULL
			AND %%[1]s != ''
		ORDER BY value`, sdmxDateStatementHint, timeSeriesTable, observationTable, joinHint) + "\n"
	}
	typeFirstContainedInPlace := newContainedInPlaceAccessPathStatements(cfg, typeFirstPlacesCTE)
	ancestorFirstContainedInPlace := newContainedInPlaceAccessPathStatements(cfg, ancestorFirstPlacesCTE)
	materializedContainedInPlace := newContainedInPlaceAccessPathStatements(cfg, materializedPlacesCTE)

	return &ObservationStatements{
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
		getObsBothWithDate: fmt.Sprintf(`%[3]s		WITH params AS (
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
		WHERE o.date = @date`, cfg.ObservationTable, cfg.TimeSeriesTable, dateStatementHint),

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
		getObsEntitiesOnlyWithDate: fmt.Sprintf(`%[3]s		WITH series AS (
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
		WHERE o.date = @date`, cfg.ObservationTable, cfg.TimeSeriesTable, dateStatementHint),

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
		getObsByContainedInPlaceMaterialized:  materializedContainedInPlace,
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

		getSdmxObsWithDates: fmt.Sprintf(`%[3]sWITH series AS (
			SELECT
				t.variable_measured,
				t.entity1,
				t.extra_entities_id,
				t.facet_id,
				t.provenance,
				t.facet,
				t.entities
			FROM %[2]s t
			WHERE %%s
		)
		SELECT
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facet_id,
			ANY_VALUE(t.provenance) AS provenance,
			COALESCE(
				ARRAY_AGG(STRUCT(o.date AS date, o.value AS str_value) ORDER BY o.date),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			ANY_VALUE(t.facet) AS facets,
			ANY_VALUE(t.entities) AS entities
		FROM series t
		JOIN@{JOIN_METHOD=APPLY_JOIN} %[1]s o
		USING (variable_measured, entity1, extra_entities_id, facet_id)
		WHERE o.date IN UNNEST(@time_periods)
		GROUP BY
			t.variable_measured,
			t.entity1,
			t.extra_entities_id,
			t.facet_id`, cfg.ObservationTable, cfg.TimeSeriesTable, sdmxDateStatementHint),

		getSdmxObsLatest: fmt.Sprintf(`		SELECT
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

		getSdmxAvailabilityWithDates: buildSdmxAvailabilityWithDates(
			cfg.TimeSeriesTable,
			cfg.ObservationTable,
			"",
		),

		getSdmxAvailabilityWithDatesMergeBaseTable: buildSdmxAvailabilityWithDates(
			cfg.TimeSeriesTable+"@{FORCE_INDEX=_BASE_TABLE}",
			cfg.ObservationTable+"@{FORCE_INDEX=_BASE_TABLE}",
			"@{JOIN_METHOD=MERGE_JOIN}",
		),

		sdmxContainedPlacesEdge: `SELECT DISTINCT contained.subject_id AS place_id
			FROM Edge typed
			JOIN@{FORCE_JOIN_ORDER=TRUE} Edge contained ON contained.subject_id = typed.subject_id
			WHERE contained.predicate = '%[1]s'
				AND contained.object_id = @%[2]s
				AND typed.predicate = '%[3]s'
				AND typed.object_id = @%[4]s`,

		sdmxContainedPlacesLinkedEdge: `SELECT DISTINCT child AS place_id
			FROM LinkedEdge
			WHERE predicate = '%[1]s'
				AND ancestor = @%[2]s
				AND child_type = @%[3]s`,

		// Force typeOf edges as the left input so Spanner filters by place type
		// before containment. A broad containment lookup can return every place
		// under an ancestor and usually produces a larger intermediate result.
		sdmxContainedPlacesCTE: `%[1]s AS (
			%[2]s
		)`,

		sdmxContainedPlacesWithRemoteCTE: `%[1]s AS (
			%[2]s
			UNION DISTINCT
			SELECT place_id
			FROM UNNEST(@%[3]s) AS place_id
		)`,

		sdmxContainedSeriesCTE: fmt.Sprintf(`series AS (
			SELECT
				%%[6]s
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
			COALESCE(
				ARRAY_AGG(STRUCT(o.date AS date, o.value AS str_value)),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
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

		getSdmxContainedInPlaceWithDates: fmt.Sprintf(`		%%[1]sWITH %%[2]s,
		%%[3]s
		SELECT
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facet_id,
			ANY_VALUE(t.provenance) AS provenance,
			COALESCE(
				ARRAY_AGG(STRUCT(o.date AS date, o.value AS str_value) ORDER BY o.date),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			ANY_VALUE(t.facet) AS facets,
			ANY_VALUE(t.entities) AS entities
		FROM series t
		JOIN@{JOIN_METHOD=APPLY_JOIN} %s o
		USING (variable_measured, entity1, extra_entities_id, facet_id)
		WHERE o.date IN UNNEST(@time_periods)
		GROUP BY
			t.variable_measured,
			t.entity1,
			t.extra_entities_id,
			t.facet_id`, cfg.ObservationTable),

		getSdmxContainedInPlaceLatest: fmt.Sprintf(`		%%[1]sWITH %%[2]s,
		%%[3]s
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
						FROM %s o
						WHERE o.variable_measured = t.variable_measured
							AND o.entity1 = t.entity1
							AND o.extra_entities_id = t.extra_entities_id
							AND o.facet_id = t.facet_id
						ORDER BY o.date DESC
						LIMIT 1
					)
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facet AS facets,
			t.entities
		FROM series t`, cfg.ObservationTable),

		getSdmxContainedAvailability: `		%[1]sWITH %[2]s,
			%[3]s
			SELECT DISTINCT t.value AS value
			FROM series t
			WHERE t.value IS NOT NULL
				AND t.value != ''
			ORDER BY value
`,

		getSdmxContainedAvailabilityWithDates: fmt.Sprintf(`		%%[1]sWITH %%[2]s,
			%%[3]s
			SELECT DISTINCT t.value AS value
			FROM series t
			JOIN %s o
			USING (variable_measured, entity1, extra_entities_id, facet_id)
			WHERE o.date IN UNNEST(@time_periods)
				AND t.value IS NOT NULL
				AND t.value != ''
			ORDER BY value
`, cfg.ObservationTable),

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
		SELECT variable_measured, entity FROM slot3`, cfg.TimeSeriesTable, entity2IndexHint, entity3IndexHint),

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
		SELECT variable_measured, entity FROM slot3`, cfg.TimeSeriesTable, entity2IndexHint, entity3IndexHint),

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
		SELECT variable_measured, entity FROM slot3`, cfg.TimeSeriesTable, entity2IndexHint, entity3IndexHint),

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
		ORDER BY variable, entity`, cfg.TimeSeriesTable, entity2IndexHint, entity3IndexHint),

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
					COUNT(DISTINCT e.subject_id) AS descendent_stat_var_count
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
					COUNT(DISTINCT e.subject_id) AS descendent_stat_var_count
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

		filterDescendentStatVarsByTimeSeriesBuildRight: `JOIN@{
					JOIN_METHOD=HASH_JOIN,
					HASH_JOIN_BUILD_SIDE=BUILD_RIGHT
				} (
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
					) AS ts`, cfg.TimeSeriesTable, entity2IndexHint, entity3IndexHint),

		joinDescendentStatVarsByProvenance: `
					JOIN Edge@{FORCE_INDEX=InEdge} e1
					ON ts.provenance = e1.subject_id`,

		filterDescendentStatVarsByProvenancePredicate: "e1.predicate = @predicate",

		filterDescendentStatVarsByProvenanceObject: "e1.object_id = @provenance",

		filterDescendentStatVarsByOnlyProvenance: `JOIN@{JOIN_TYPE=HASH_JOIN} (
					SELECT summary.key AS variable_measured
					FROM Edge@{FORCE_INDEX=InEdge} AS provenance_edge
					JOIN KeyValueStore@{FORCE_INDEX=KeyValueStoreByProvenance} AS summary
					ON summary.provenance = provenance_edge.subject_id
					WHERE provenance_edge.predicate = @predicate
						AND provenance_edge.object_id = @provenance
						AND summary.type = 'ProvenanceSummary'
					GROUP BY summary.key
				) AS o
					ON o.variable_measured = e.subject_id`,

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
		attachSVGs: `SELECT
				node AS child_svg,
				node AS svg
				FROM UNNEST(@nodes) AS node`,
		getSVGChildren: `		SELECT DISTINCT
			n.subject_id,
			IFNULL(n.name, '') AS name,
			e.predicate,
			'' AS definition
		FROM Node n
		JOIN (
			SELECT subject_id, predicate FROM Edge@{FORCE_INDEX=InEdge}
			WHERE predicate IN ('memberOf', 'specializationOf')
				AND object_id = @node
		) e ON n.subject_id = e.subject_id`,
		getSVGChildrenWithDefinitions: `		SELECT DISTINCT
			n.subject_id,
			IFNULL(n.name, '') AS name,
			e.predicate,
			IFNULL((
				SELECT n_def.value
				FROM Edge e_def
				JOIN Node n_def ON e_def.object_id = n_def.subject_id
				WHERE e_def.subject_id = n.subject_id
				AND e_def.predicate = 'definition'
				LIMIT 1
			), '') AS definition
		FROM Node n
		JOIN (
			SELECT subject_id, predicate FROM Edge@{FORCE_INDEX=InEdge}
			WHERE predicate IN ('memberOf', 'specializationOf')
				AND object_id = @node
		) e ON n.subject_id = e.subject_id`,
		getFilteredChildSVs: `		SELECT
			n.subject_id,
			IFNULL(n.name, '') AS name,
			'' AS definition
		FROM Node n
		JOIN (
			SELECT
				e.subject_id AS subject_id
			FROM Edge e
			%s
			WHERE e.subject_id IN (
				SELECT DISTINCT subject_id
				FROM Edge
				WHERE object_id = @node
					AND predicate = 'memberOf'
			)
			GROUP BY
				e.subject_id
		) e_existence 
			ON n.subject_id = e_existence.subject_id`,
		getFilteredChildSVsWithDefinitions: `		SELECT
			n.subject_id,
			IFNULL(n.name, '') AS name,
			IFNULL((
				SELECT n_def.value
				FROM Edge e_def
				JOIN Node n_def ON e_def.object_id = n_def.subject_id
				WHERE e_def.subject_id = n.subject_id
				AND e_def.predicate = 'definition'
				LIMIT 1
			), '') AS definition
		FROM Node n
		JOIN (
			SELECT
				e.subject_id AS subject_id
			FROM Edge e
			%s
			WHERE e.subject_id IN (
				SELECT DISTINCT subject_id
				FROM Edge
				WHERE object_id = @node
					AND predicate = 'memberOf'
			)
			GROUP BY
				e.subject_id
		) e_existence 
			ON n.subject_id = e_existence.subject_id`,
		getFilteredChildSVGs: `		SELECT
			n.subject_id,
			IFNULL(n.name, '') AS name,
			e_counts.descendent_stat_var_count
		FROM Node n
		JOIN (
			SELECT
				e.object_id AS subject_id,
				COUNT(DISTINCT e.subject_id) AS descendent_stat_var_count
			FROM Edge e
			%s
			WHERE e.predicate = 'linkedMemberOf'
				AND e.object_id IN (
					SELECT DISTINCT subject_id
					FROM Edge
					WHERE object_id = @node
						AND predicate = 'specializationOf'
					UNION ALL
					SELECT @node AS subject_id
				)
			GROUP BY
				e.object_id
		) e_counts
			ON n.subject_id = e_counts.subject_id`,
		getFilteredTopic: `		SELECT
			e.object_id AS subject_id,
			COUNT(DISTINCT e.subject_id) AS descendent_stat_var_count
		FROM Edge@{FORCE_INDEX=InEdge} e
		%[1]s
		WHERE e.predicate = 'linkedMember'
			AND e.object_id %[2]s
		GROUP BY
			e.object_id`,
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

const materializedPlacesCTE = `places AS (
			SELECT DISTINCT child AS place_id
			FROM LinkedEdge
			WHERE predicate = 'containedInPlace'
			AND ancestor = @ancestor
			AND child_type = @childPlaceType
		)`

func newContainedInPlaceAccessPathStatements(cfg TableConfig, placesCTE string) containedInPlaceAccessPathStatements {
	entityScanSource := fmt.Sprintf(
		"%s@{FORCE_INDEX=%s, SEEKABLE_KEY_SIZE=1}",
		cfg.TimeSeriesTable,
		cfg.TimeSeriesByEntity1Index,
	)
	if cfg.spannerEmulatorCompatibility {
		// The emulator does not implement SEEKABLE_KEY_SIZE. FORCE_INDEX still
		// exercises the entity1 index while production keeps the seekable prefix.
		entityScanSource = fmt.Sprintf("%s@{FORCE_INDEX=%s}", cfg.TimeSeriesTable, cfg.TimeSeriesByEntity1Index)
	}
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
			entityScanSource,
		),
	}
}

// columnarBatchStatementHint returns the statement hint for queries that read
// many dates. The emulator rejects both SCAN_METHOD=COLUMNAR and
// EXECUTION_METHOD, so in emulator mode the whole hint is omitted.
func columnarBatchStatementHint(cfg TableConfig) string {
	if cfg.spannerEmulatorCompatibility {
		return ""
	}
	return "\t\t@{SCAN_METHOD=COLUMNAR, EXECUTION_METHOD=BATCH}\n"
}

func newContainedInPlaceStatements(cfg TableConfig, placesCTE, timeSeriesSource string) containedInPlaceStatements {
	statementHint := columnarBatchStatementHint(cfg)
	return containedInPlaceStatements{
		// Join all dates before aggregation to optimize total result throughput.
		all: statementHint + fmt.Sprintf(`		WITH %[3]s,
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
		withDate: statementHint + fmt.Sprintf(`		WITH %[3]s,
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
		latest: statementHint + fmt.Sprintf(`		WITH %[3]s,
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

func validateTableConfig(cfg TableConfig) error {
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
		return fmt.Errorf("NewObservationStatements: missing required TableConfig fields: %s", strings.Join(missing, ", "))
	}
	return nil
}
