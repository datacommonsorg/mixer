		SELECT DISTINCT
			n.subject_id,
			IFNULL(n.name, '') AS name,
			e.predicate,
			'' AS definition
		FROM Node n
		JOIN (
			SELECT subject_id, predicate FROM Edge@{FORCE_INDEX=InEdge}
			WHERE predicate IN ('memberOf', 'specializationOf')
				AND object_id = 'dc/g/Demographics'
		) e ON n.subject_id = e.subject_id