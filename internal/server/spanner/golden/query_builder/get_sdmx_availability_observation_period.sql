		SELECT DISTINCT t.observation_period AS value
		FROM TimeSeries t
		WHERE ((t.variable_measured = "Count_Household") OR (t.variable_measured = "Count_Person"))
			AND t.observation_period IS NOT NULL
			AND t.observation_period != ''
		ORDER BY value
