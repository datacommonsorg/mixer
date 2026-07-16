		SELECT DISTINCT t.observation_period AS value
		FROM TimeSeries t
		WHERE (t.variable_measured IN ('Count_Household','Count_Person'))
			AND t.observation_period IS NOT NULL
			AND t.observation_period != ''
		ORDER BY value
