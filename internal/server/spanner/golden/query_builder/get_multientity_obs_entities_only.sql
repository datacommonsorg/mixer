		SELECT 
			t.variable_measured,
			t.entity1 AS observation_about,
			COALESCE(
				(
					SELECT ARRAY_AGG(STRUCT(date, str_value) ORDER BY date ASC) 
					FROM Observation_final_v2 o
					WHERE o.variable_measured = t.variable_measured
						AND o.entity1 = t.entity1
						AND o.extra_entities_id = t.extra_entities_id
						AND o.facets_id = t.facets_id
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facets
		FROM TimeSeries_final_v2 t
		WHERE t.entity1 IN ('geoId/01001','geoId/02013')