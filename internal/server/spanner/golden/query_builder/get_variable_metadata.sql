		SELECT
			variable_measured,
			import_name,
			facet_id,
			observation_period,
			measurement_method,
			unit,
			scaling_factor,
			is_dc_aggregate,
			total_observations,
			observed_places,
			min_date,
			max_date,
			place_types
		FROM
			VariableMetadata
		WHERE
			variable_measured IN ('Count_Household_FamilyHousehold','Count_Household_HasComputer')