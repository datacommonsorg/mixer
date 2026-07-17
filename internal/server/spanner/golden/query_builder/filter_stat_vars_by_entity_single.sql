		SELECT DISTINCT
			variable_measured,
			observation_about
		FROM
			Observation
		WHERE
			variable_measured = 'Count_Person'
			AND observation_about = 'geoId/06'