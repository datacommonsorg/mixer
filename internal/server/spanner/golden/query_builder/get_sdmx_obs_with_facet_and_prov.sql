		SELECT 
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facet_id,
			t.provenance,
			COALESCE(
				(
					SELECT ARRAY_AGG(STRUCT(date, value AS str_value))
					FROM Observation o
					WHERE o.variable_measured = t.variable_measured
						AND o.entity1 = t.entity1
						AND o.extra_entities_id = t.extra_entities_id
						AND o.facet_id = t.facet_id
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facet AS facets,
			t.entities
		FROM TimeSeries t
		WHERE t.variable_measured IN ('var1') AND t.entity2 IN ('country/PRT','country/SGP') AND t.facet_id IN ('facet','alternate-facet') AND t.measurement_method IN ('Census','Survey') AND t.observation_period IN ('P1Y','P1M') AND t.entity1 IN ('country/AGO','country/BRA') AND t.provenance IN ('dc/base/WTO_TradeConnectivity','dc/base/UN_Trade') AND t.unit IN ('Percent','Count')