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
				ON t.variable_measured = 'AirPollutant_Cancer_Risk'
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
						FROM Observation o
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
		FROM series t