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
			variable_measured = 'AirPollutant_Cancer_Risk'
			AND observation_about IN ('geoId/01001','geoId/02013')