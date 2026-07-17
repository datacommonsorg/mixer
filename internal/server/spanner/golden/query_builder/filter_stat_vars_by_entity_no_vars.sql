		SELECT DISTINCT
			variable_measured,
			observation_about
		FROM
			Observation
		WHERE
			observation_about = 'geoId/06'