		SELECT DISTINCT t.entity1 AS value
		FROM TimeSeries t
		WHERE (t.variable_measured IN ('var1','var2') AND t.entity1 IN ('country/PRT','country/SGP') AND t.entity2 IN ('country/AGO','country/BRA') AND t.measurement_method IN ('Census','Survey') AND t.observation_period IN ('P1Y','P1M') AND t.provenance IN ('dc/base/one','dc/base/two') AND t.unit IN ('Percent','Count'))
			AND t.entity1 IS NOT NULL
			AND t.entity1 != ''
		ORDER BY value
