		@{SCAN_METHOD=COLUMNAR, EXECUTION_METHOD=BATCH}
		WITH series AS (
			SELECT
				t.variable_measured,
				t.entity1,
				t.extra_entities_id,
				t.facet_id,
				t.provenance,
				t.facet,
				t.entities
			FROM TimeSeries t
			WHERE t.variable_measured = 'var1' AND t.entity1 = 'wikidataId/Q119158'
		)
		SELECT
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facet_id,
			ANY_VALUE(t.provenance) AS provenance,
			ARRAY_AGG(STRUCT(o.date AS date, o.value AS str_value) ORDER BY o.date) AS dates_and_values,
			ANY_VALUE(t.facet) AS facets,
			ANY_VALUE(t.entities) AS entities
		FROM series t
		JOIN@{JOIN_METHOD=APPLY_JOIN} Observation o
		USING (variable_measured, entity1, extra_entities_id, facet_id)
		WHERE o.date IN ('2020','2022')
		GROUP BY
			t.variable_measured,
			t.entity1,
			t.extra_entities_id,
			t.facet_id