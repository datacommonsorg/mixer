		@{SCAN_METHOD=COLUMNAR, EXECUTION_METHOD=BATCH}
		WITH contained_places_0 AS (
			SELECT DISTINCT child AS place_id
			FROM LinkedEdge
			WHERE predicate = 'containedInPlace'
				AND ancestor = 'country/USA'
				AND child_type = 'County'
		),
			series AS (
			SELECT
				t.entity1 AS value
			FROM contained_places_0 anchor
			JOIN@{JOIN_METHOD=APPLY_JOIN} TimeSeries@{FORCE_INDEX=_BASE_TABLE} t
				ON t.entity1 = anchor.place_id
				AND t.variable_measured = 'var1'
		)
			SELECT DISTINCT t.value AS value
			FROM series t
			WHERE t.value IS NOT NULL
				AND t.value != ''
			ORDER BY value
