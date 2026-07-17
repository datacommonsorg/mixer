		@{SCAN_METHOD=COLUMNAR, EXECUTION_METHOD=BATCH}
		WITH places AS (
			SELECT DISTINCT e.subject_id AS place_id
			FROM Edge e
			JOIN Edge e2 ON e.subject_id = e2.subject_id
			WHERE e.predicate = 'linkedContainedInPlace'
				AND e.object_id = 'geoId/10'
				AND e2.predicate = 'typeOf'
				AND e2.object_id = 'County'
		),
		series AS (
			SELECT
				t.variable_measured,
				t.entity1,
				t.extra_entities_id,
				t.facet_id,
				t.provenance,
				t.facet
			FROM places p
			JOIN@{JOIN_METHOD=APPLY_JOIN} TimeSeries@{FORCE_INDEX=_BASE_TABLE} t
				ON t.variable_measured IN ('AirPollutant_Cancer_Risk')
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
		JOIN@{JOIN_METHOD=APPLY_JOIN} Observation o
		USING (variable_measured, entity1, extra_entities_id, facet_id)
		GROUP BY
			t.variable_measured,
			t.entity1,
			t.extra_entities_id,
			t.facet_id