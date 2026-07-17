		SELECT DISTINCT t.measurement_method AS value
		FROM TimeSeries t
		WHERE (t.variable_measured IN ('Count_Household','Count_Person'))
			AND t.measurement_method IS NOT NULL
			AND t.measurement_method != ''
		ORDER BY value
