		SELECT DISTINCT t.measurement_method AS value
		FROM TimeSeries t
		WHERE ((t.variable_measured = "Count_Household") OR (t.variable_measured = "Count_Person"))
			AND t.measurement_method IS NOT NULL
			AND t.measurement_method != ''
		ORDER BY value
