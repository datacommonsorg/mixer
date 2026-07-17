		SELECT DISTINCT
			variable_measured,
			observation_about
		FROM
			Observation
		WHERE
			variable_measured IN ('Count_Person','Median_Income_Person')
			AND observation_about = 'geoId/06'