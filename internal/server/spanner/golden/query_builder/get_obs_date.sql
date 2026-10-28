		@{SCAN_METHOD=COLUMNAR, EXECUTION_METHOD=BATCH}
		WITH params AS (
			SELECT var, ent
			FROM UNNEST(['AirPollutant_Cancer_Risk']) AS var
			CROSS JOIN UNNEST(['geoId/01001','geoId/02013']) AS ent
		),
		series AS (
			SELECT
				t.variable_measured,
				t.entity1,
				t.extra_entities_id,
				t.facet_id,
				t.provenance,
				t.facet
			FROM params p
			JOIN@{JOIN_METHOD=APPLY_JOIN} TimeSeries t
				ON t.variable_measured = p.var AND t.entity1 = p.ent
		)
		SELECT
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facet_id,
			t.provenance,
			ARRAY(
				SELECT AS STRUCT
					o.date AS date,
					o.value AS str_value
			) AS dates_and_values,
			t.facet AS facets
		FROM series t
		JOIN@{JOIN_METHOD=APPLY_JOIN} Observation o
		USING (variable_measured, entity1, extra_entities_id, facet_id)
		WHERE o.date = '2015'