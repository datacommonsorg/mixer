		@{SCAN_METHOD=COLUMNAR, EXECUTION_METHOD=BATCH}
		WITH contained_places_0 AS (
			SELECT DISTINCT child AS place_id
			FROM LinkedEdge
			WHERE predicate = 'containedInPlace'
				AND ancestor = 'country/USA'
				AND child_type = 'State'
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
			JOIN@{JOIN_METHOD=APPLY_JOIN} TimeSeries@{FORCE_INDEX=TimeSeriesByEntity3} t
				ON t.entity3 = anchor.place_id
				AND t.variable_measured = 'var1'
			WHERE t.entity3 IS NOT NULL
				AND t.entity2 IS NOT NULL
				AND t.entity2 IN (SELECT place_id FROM contained_places_0)
		)
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
		JOIN@{JOIN_METHOD=APPLY_JOIN} Observation o
		USING (variable_measured, entity1, extra_entities_id, facet_id)
		GROUP BY
			t.variable_measured,
			t.entity1,
			t.extra_entities_id,
			t.facet_id