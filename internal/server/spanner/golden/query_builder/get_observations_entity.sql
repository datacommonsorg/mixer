		SELECT
			variable_measured,
			observation_about,
			observations,
			import_name,
			observation_period,
			measurement_method,
			unit,
			scaling_factor,
			provenance_url,
			facet_id
		FROM 
			Observation
		WHERE
			observation_about IN ('wikidataId/Q341968')