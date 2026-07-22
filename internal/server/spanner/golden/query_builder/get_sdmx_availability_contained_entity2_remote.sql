		@{SCAN_METHOD=COLUMNAR, EXECUTION_METHOD=BATCH}
		WITH contained_places_0 AS (
			SELECT DISTINCT contained.subject_id AS place_id
			FROM Edge typed
			JOIN@{FORCE_JOIN_ORDER=TRUE} Edge contained ON contained.subject_id = typed.subject_id
			WHERE contained.predicate = 'linkedContainedInPlace'
				AND contained.object_id = 'Earth'
				AND typed.predicate = 'typeOf'
				AND typed.object_id = 'Country'
			UNION DISTINCT
			SELECT place_id
			FROM UNNEST(['country/USA']) AS place_id
		),
			series AS (
			SELECT
				t.entity1 AS value
			FROM contained_places_0 anchor
			JOIN@{JOIN_METHOD=APPLY_JOIN} TimeSeries@{FORCE_INDEX=TimeSeriesByEntity2} t
				ON t.entity2 = anchor.place_id
				AND t.variable_measured = 'var1'
			WHERE t.entity2 IS NOT NULL
		)
			SELECT DISTINCT t.value AS value
			FROM series t
			WHERE t.value IS NOT NULL
				AND t.value != ''
			ORDER BY value
