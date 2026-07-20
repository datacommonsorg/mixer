		SELECT DISTINCT t.variable_measured AS value
		FROM TimeSeries t
		WHERE (t.variable_measured IN ('Count_Household','Count_Person'))
			AND t.variable_measured IS NOT NULL
			AND t.variable_measured != ''
		ORDER BY value
