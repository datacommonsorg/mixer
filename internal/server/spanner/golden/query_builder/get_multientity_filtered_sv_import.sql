		SELECT
			n.subject_id,
			n.name,
			'' AS definition
		FROM Node n
		JOIN (
			SELECT
				e.subject_id AS subject_id
			FROM Edge e
			JOIN@{JOIN_TYPE=HASH_JOIN} (
					SELECT ts.variable_measured
					FROM TimeSeries AS ts
					JOIN Edge@{FORCE_INDEX=InEdge} e1
					ON ts.provenance = e1.subject_id
					WHERE e1.predicate = 'source'
					  AND e1.object_id = 'dc/s/WorldBank'
					GROUP BY ts.variable_measured
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