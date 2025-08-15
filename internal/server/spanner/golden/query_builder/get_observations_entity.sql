		SELECT
			*
		FROM 
			Observation
		WHERE
			observation_about IN UNNEST(@entities)