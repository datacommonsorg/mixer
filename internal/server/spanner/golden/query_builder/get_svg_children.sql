		SELECT DISTINCT
			n.subject_id,
			n.name,
			e.predicate
		FROM Node n
		JOIN (
			SELECT subject_id, predicate FROM Edge@{FORCE_INDEX=InEdge}
			WHERE predicate IN ('memberOf', 'specializationOf')
				AND object_id = 'dc/g/Demographics'
		) e ON n.subject_id = e.subject_id