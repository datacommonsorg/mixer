		SELECT DISTINCT t.entity1 AS value
		FROM TimeSeries t
		WHERE (t.variable_measured IN ('Count_Household','Count_Person'))
			AND t.entity1 IS NOT NULL
			AND t.entity1 != ''
		ORDER BY value
