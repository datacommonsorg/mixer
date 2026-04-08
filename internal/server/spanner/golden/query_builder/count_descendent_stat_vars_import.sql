		SELECT
			e.object_id,
			COUNT(e.subject_id) AS descendent_stat_vars
		FROM Edge@{FORCE_INDEX=InEdge} e
		JOIN@{JOIN_TYPE=HASH_JOIN} (
			SELECT variable_measured
			FROM Observation
			JOIN Edge@{FORCE_INDEX=InEdge} e1
			ON import_name = SUBSTR(e1.subject_id, 9)
			WHERE e1.predicate = 'source'
				AND e1.object_id = 'dc/s/WorldBank'
			GROUP BY variable_measured	
		) o ON o.variable_measured = e.subject_id
		WHERE
		 	e.predicate = 'linkedMemberOf'
			AND e.object_id = 'dc/g/Demographics'
			GROUP BY e.object_id