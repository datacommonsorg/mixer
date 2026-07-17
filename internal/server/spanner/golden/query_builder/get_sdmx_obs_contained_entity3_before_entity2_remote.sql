		@{spanner_emulator.disable_query_null_filtered_index_check=true}
		WITH contained_places_0 AS (
			SELECT DISTINCT contained.subject_id AS place_id
			FROM Edge contained
			JOIN Edge typed ON contained.subject_id = typed.subject_id
			WHERE contained.predicate = 'linkedContainedInPlace'
				AND contained.object_id = 'country/USA'
				AND typed.predicate = 'typeOf'
				AND typed.object_id = 'State'
			UNION DISTINCT
			SELECT place_id
			FROM UNNEST(['country/CAN','country/USA']) AS place_id
		),
		series AS (
			SELECT
				t.variable_measured,
				t.entity1,
				t.extra_entities_id,
				t.facet_id,
				t.provenance,
				t.facet,
				t.entities
			FROM contained_places_0 anchor
			JOIN@{JOIN_METHOD=APPLY_JOIN, FORCE_JOIN_ORDER=TRUE} TimeSeries@{FORCE_INDEX=TimeSeriesByEntity3} t
				ON t.entity3 = anchor.place_id
				AND t.variable_measured IN ('var1')
			WHERE t.entity3 IS NOT NULL
				AND t.entity2 IS NOT NULL
				AND t.entity2 IN (SELECT place_id FROM contained_places_0)
		)
		SELECT
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facet_id,
			ANY_VALUE(t.provenance) AS provenance,
			ARRAY_AGG(STRUCT(o.date AS date, o.value AS str_value)) AS dates_and_values,
			ANY_VALUE(t.facet) AS facets,
			ANY_VALUE(t.entities) AS entities
		FROM series t
		JOIN@{JOIN_METHOD=APPLY_JOIN, FORCE_JOIN_ORDER=TRUE} Observation o
		USING (variable_measured, entity1, extra_entities_id, facet_id)
		GROUP BY
			t.variable_measured,
			t.entity1,
			t.extra_entities_id,
			t.facet_id