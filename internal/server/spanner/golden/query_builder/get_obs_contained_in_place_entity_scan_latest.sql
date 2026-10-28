		@{SCAN_METHOD=COLUMNAR, EXECUTION_METHOD=BATCH}
		WITH places AS (
			SELECT DISTINCT contained.subject_id AS place_id
			FROM Edge typed
			JOIN@{FORCE_JOIN_ORDER=TRUE} Edge contained ON contained.subject_id = typed.subject_id
			WHERE typed.predicate = 'typeOf'
				AND typed.object_id = 'County'
				AND contained.predicate = 'linkedContainedInPlace'
				AND contained.object_id = 'geoId/10'
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
			JOIN@{JOIN_METHOD=APPLY_JOIN} TimeSeries@{FORCE_INDEX=TimeSeriesByEntity1, SEEKABLE_KEY_SIZE=1} t
				ON t.variable_measured IN ('AirPollutant_Cancer_Risk','Count_Person')
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