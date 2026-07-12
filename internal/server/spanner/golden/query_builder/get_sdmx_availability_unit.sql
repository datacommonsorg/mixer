		SELECT DISTINCT t.unit AS value
		FROM TimeSeries t
		WHERE ((t.variable_measured = "Count_Household") OR (t.variable_measured = "Count_Person"))
			AND t.unit IS NOT NULL
			AND t.unit != ''
		ORDER BY value
