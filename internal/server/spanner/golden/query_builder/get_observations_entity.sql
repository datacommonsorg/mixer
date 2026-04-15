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
			is_dc_aggregate,
			facet_id
		FROM 
			Observation@{FORCE_INDEX=_BASE_TABLE}
		WHERE
			observation_about = 'wikidataId/Q341968'