		SELECT DISTINCT t.unit AS value
		FROM TimeSeries t
		WHERE t.variable_measured IN ('Count_Person','Count_Household')
			AND t.unit IS NOT NULL
			AND t.unit != ""
		ORDER BY value
