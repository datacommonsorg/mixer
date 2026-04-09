		SELECT
			n.subject_id,
			n.name,
			e_counts.descendent_stat_var_count
		FROM Node n
		JOIN (
			SELECT
				e.object_id AS subject_id,
				COUNT(e.subject_id) AS descendent_stat_var_count
			FROM Edge e
			JOIN@{JOIN_TYPE=HASH_JOIN} (
				SELECT variable_measured
				FROM Observation 
				WHERE observation_about IN ('country/USA','country/IND')
				GROUP BY variable_measured
			) o ON o.variable_measured = e.subject_id
			WHERE e.predicate = 'linkedMemberOf'
				AND e.object_id IN (
					SELECT DISTINCT subject_id
					FROM Edge
					WHERE object_id = 'dc/g/Demographics'
						AND predicate = 'specializationOf'
					UNION ALL
					SELECT 'dc/g/Demographics' AS subject_id
				)
			GROUP BY
				e.object_id
		) e_counts
			ON n.subject_id = e_counts.subject_id