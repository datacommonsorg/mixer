		SELECT
			e.object_id AS subject_id,
			COUNT(DISTINCT e.subject_id) AS descendent_stat_var_count
		FROM Edge@{FORCE_INDEX=InEdge} e
		JOIN@{JOIN_TYPE=HASH_JOIN} (
					SELECT ts.variable_measured
					FROM TimeSeries AS ts
					JOIN Edge@{FORCE_INDEX=InEdge} e1
					ON ts.provenance = e1.subject_id
					WHERE e1.predicate = 'source'
					  AND e1.object_id = 'dc/s/WorldBank'
					GROUP BY ts.variable_measured
				) o ON o.variable_measured = e.subject_id
		WHERE e.predicate = 'linkedMember'
			AND e.object_id = 'dc/topic/Demographics'
		GROUP BY
			e.object_id