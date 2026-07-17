		SELECT
			n.subject_id,
			IFNULL(n.name, '') AS name,
			e_counts.descendent_stat_var_count
		FROM Node n
		JOIN (
			SELECT
				e.object_id AS subject_id,
				COUNT(e.subject_id) AS descendent_stat_var_count
			FROM Edge e
			JOIN@{JOIN_TYPE=HASH_JOIN} (
					SELECT ts.variable_measured
					FROM TimeSeries AS ts
					JOIN Edge@{FORCE_INDEX=InEdge} e1
					ON ts.provenance = e1.subject_id
					WHERE e1.predicate = 'source'
					  AND e1.object_id = 'dc/s/WorldBank'
					GROUP BY ts.variable_measured
					HAVING COUNT(DISTINCT e1.subject_id) >= 2
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