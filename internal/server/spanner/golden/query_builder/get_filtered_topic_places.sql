		SELECT
			e.object_id AS subject_id,
			COUNT(e.subject_id) AS descendent_stat_var_count
		FROM Edge@{FORCE_INDEX=InEdge} e
		JOIN@{JOIN_TYPE=HASH_JOIN} (
				SELECT variable_measured
				FROM Observation 
				WHERE observation_about IN ('country/CAN','country/IND')
				GROUP BY variable_measured
			) o ON o.variable_measured = e.subject_id
		WHERE e.predicate = 'linkedMember'
			AND e.object_id = 'dc/topic/Demographics'
		GROUP BY
			e.object_id