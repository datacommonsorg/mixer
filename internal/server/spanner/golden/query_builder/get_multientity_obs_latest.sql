		WITH params AS (
			SELECT var, ent
			FROM UNNEST(['AirPollutant_Cancer_Risk']) AS var
			CROSS JOIN ('geoId/01001','geoId/02013') AS ent
		)
		SELECT
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facet_id,
			t.provenance,
			COALESCE(
				(
					SELECT ARRAY(
						SELECT AS STRUCT date, value AS str_value, attributes
						FROM Observation o
						WHERE o.variable_measured = t.variable_measured
							AND o.entity1 = t.entity1
							AND o.extra_entities_id = t.extra_entities_id
							AND o.facet_id = t.facet_id
						ORDER BY date DESC
						LIMIT 1
					)
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value, CAST(NULL AS JSON) AS attributes FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facet AS facets
		FROM params p
		JOIN@{JOIN_METHOD=APPLY_JOIN, FORCE_JOIN_ORDER=TRUE} TimeSeries t
			ON t.variable_measured = p.var AND t.entity1 = p.ent