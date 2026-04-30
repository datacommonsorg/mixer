		SELECT DISTINCT
			n.subject_id,
			n.name,
			e.predicate,
			(
				SELECT n_def.value
				FROM Edge e_def
				JOIN Node n_def ON e_def.object_id = n_def.subject_id
				WHERE e_def.subject_id = n.subject_id
				AND e_def.predicate = 'definition'
				LIMIT 1
			) AS definition
		FROM Node n
		JOIN (
			SELECT subject_id, predicate FROM Edge@{FORCE_INDEX=InEdge}
			WHERE predicate IN ('memberOf', 'specializationOf')
				AND object_id = 'dc/g/Demographics'
		) e ON n.subject_id = e.subject_id