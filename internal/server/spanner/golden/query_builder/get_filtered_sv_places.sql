		SELECT
			n.subject_id,
			n.name,
			IFNULL((
				SELECT n_def.value
				FROM Edge e_def
				JOIN Node n_def ON e_def.object_id = n_def.subject_id
				WHERE e_def.subject_id = n.subject_id
				AND e_def.predicate = 'definition'
				LIMIT 1
			), '') AS definition
		FROM Node n
		JOIN (
			SELECT
				e.subject_id AS subject_id
			FROM Edge e
			JOIN@{JOIN_TYPE=HASH_JOIN} (
				SELECT variable_measured
				FROM Observation 
				WHERE observation_about IN ('country/USA','country/IND')
				GROUP BY variable_measured
			) o ON o.variable_measured = e.subject_id
			WHERE e.subject_id IN (
				SELECT DISTINCT subject_id
				FROM Edge
				WHERE object_id = 'dc/g/Demographics'
					AND predicate = 'memberOf'
			)
			GROUP BY
				e.subject_id
		) e_existence 
			ON n.subject_id = e_existence.subject_id