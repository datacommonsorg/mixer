		@{SCAN_METHOD=COLUMNAR, EXECUTION_METHOD=BATCH}
		WITH contained_places_0 AS (
			SELECT DISTINCT child AS place_id
			FROM LinkedEdge
			WHERE predicate = 'containedInPlace'
				AND ancestor = 'Earth'
				AND child_type = 'Country'
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
