		SELECT
			e.object_id,
			COUNT(e.subject_id) AS descendent_stat_vars
		FROM Edge@{FORCE_INDEX=InEdge} e
		JOIN@{JOIN_TYPE=HASH_JOIN} (
			SELECT variable_measured
			FROM Observation
			WHERE observation_about IN ('country/USA','country/IND')
			GROUP BY variable_measured	
		) o ON o.variable_measured = e.subject_id
		WHERE
		 	e.predicate = 'linkedMemberOf'
			AND e.object_id IN ('dc/g/Demographics','dc/g/Economy')
			GROUP BY e.object_id