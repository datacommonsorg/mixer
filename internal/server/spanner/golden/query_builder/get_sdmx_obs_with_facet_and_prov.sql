		SELECT 
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facet_id,
			t.provenance,
			COALESCE(
				(
					SELECT ARRAY_AGG(STRUCT(date, value AS str_value, CAST(NULL AS JSON) AS attributes))
					FROM Observation_final_v3 o
					WHERE o.variable_measured = t.variable_measured
						AND o.entity1 = t.entity1
						AND o.extra_entities_id = t.extra_entities_id
						AND o.facet_id = t.facet_id
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value, CAST(NULL AS JSON) AS attributes FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facet AS facets,
			t.entities
		FROM TimeSeries_final_v3 t
		WHERE (t.variable_measured = "var1" AND t.entity2 IN ('country/PRT') AND t.entity1 IN ('country/AGO') AND t.provenance IN ('dc/base/WTO_TradeConnectivity') AND JSON_VALUE(t.facet, '$.unit') IN ('Percent'))