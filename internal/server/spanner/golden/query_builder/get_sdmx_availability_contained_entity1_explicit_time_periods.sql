		@{SCAN_METHOD=COLUMNAR, EXECUTION_METHOD=BATCH}
		WITH contained_places_0 AS (
			SELECT DISTINCT contained.subject_id AS place_id
			FROM Edge typed
			JOIN@{FORCE_JOIN_ORDER=TRUE} Edge contained ON contained.subject_id = typed.subject_id
			WHERE contained.predicate = 'linkedContainedInPlace'
				AND contained.object_id = 'country/USA'
				AND typed.predicate = 'typeOf'
				AND typed.object_id = 'County'
		),
			series AS (
			SELECT
				t.variable_measured,
				t.entity1,
				t.extra_entities_id,
				t.facet_id,
				t.entity1 AS value
			FROM contained_places_0 anchor
			JOIN@{JOIN_METHOD=APPLY_JOIN} TimeSeries@{FORCE_INDEX=_BASE_TABLE} t
				ON t.entity1 = anchor.place_id
				AND t.variable_measured = 'var1'
		)
			SELECT DISTINCT t.value AS value
			FROM series t
			JOIN Observation o
			USING (variable_measured, entity1, extra_entities_id, facet_id)
			WHERE o.date IN ('2020','2022')
				AND t.value IS NOT NULL
				AND t.value != ''
			ORDER BY value
