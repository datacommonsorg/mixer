		SELECT
			*
		FROM 
			Observation
		WHERE
			variable_measured IN UNNEST(@variables)
			AND observation_about IN UNNEST(@entities)