		SELECT DISTINCT t.provenance AS value
		FROM TimeSeries t
		WHERE ((t.variable_measured = "Count_Household") OR (t.variable_measured = "Count_Person"))
			AND t.provenance IS NOT NULL
			AND t.provenance != ''
		ORDER BY value
