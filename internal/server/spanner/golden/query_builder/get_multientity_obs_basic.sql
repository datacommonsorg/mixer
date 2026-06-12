		WITH params AS (
			SELECT var, ent 
			FROM UNNEST(['AirPollutant_Cancer_Risk']) AS var 
			CROSS JOIN ('geoId/01001','geoId/02013') AS ent
		)
		SELECT 
			t.variable_measured,
			t.entity1 AS observation_about,
			COALESCE(
				(
					SELECT ARRAY_AGG(STRUCT(date, str_value)) 
					FROM Observation_final_v2 o
					WHERE o.variable_measured = t.variable_measured
						AND o.entity1 = t.entity1
						AND o.extra_entities_id = t.extra_entities_id
						AND o.facets_id = t.facets_id
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facets
		FROM params p
		JOIN@{JOIN_METHOD=APPLY_JOIN, FORCE_JOIN_ORDER=TRUE} TimeSeries_final_v2 t
			ON t.variable_measured = p.var AND t.entity1 = p.ent