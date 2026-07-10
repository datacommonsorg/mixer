		@{SCAN_METHOD=COLUMNAR, EXECUTION_METHOD=BATCH}
		WITH places AS (
			SELECT DISTINCT e.subject_id AS place_id
			FROM Edge e
			JOIN Edge e2 ON e.subject_id = e2.subject_id
			WHERE e.predicate = 'linkedContainedInPlace'
				AND e.object_id = 'geoId/10'
				AND e2.predicate = 'typeOf'
				AND e2.object_id = 'County'
		)
		SELECT
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facet_id,
			t.provenance,
			COALESCE(
				(
					SELECT ARRAY_AGG(STRUCT(date, value AS str_value))
					FROM Observation o
					WHERE o.variable_measured = t.variable_measured
						AND o.entity1 = t.entity1
						AND o.extra_entities_id = t.extra_entities_id
						AND o.facet_id = t.facet_id
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facet AS facets
		FROM places p
		CROSS JOIN ('AirPollutant_Cancer_Risk') AS target_variable
		JOIN@{JOIN_METHOD=APPLY_JOIN, FORCE_JOIN_ORDER=TRUE} TimeSeries@{FORCE_INDEX=_BASE_TABLE} t
			ON t.variable_measured = target_variable
			AND t.entity1 = p.place_id