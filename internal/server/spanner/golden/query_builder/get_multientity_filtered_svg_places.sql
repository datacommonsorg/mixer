		SELECT
			n.subject_id,
			IFNULL(n.name, '') AS name,
			e_counts.descendent_stat_var_count
		FROM Node n
		JOIN (
			SELECT
				e.object_id AS subject_id,
				COUNT(DISTINCT e.subject_id) AS descendent_stat_var_count
			FROM Edge e
			JOIN@{JOIN_TYPE=HASH_JOIN} (
					SELECT ts.variable_measured
					FROM (
						SELECT t.variable_measured, t.entity1 AS entity, t.provenance
						FROM TimeSeries AS t
						WHERE t.entity1 IN ('country/USA','country/IND')
						UNION ALL
						SELECT t.variable_measured, t.entity2 AS entity, t.provenance
						FROM TimeSeries@{FORCE_INDEX=TimeSeriesByEntity2} AS t
						WHERE t.entity2 IN ('country/USA','country/IND')
							AND t.entity2 IS NOT NULL
						UNION ALL
						SELECT t.variable_measured, t.entity3 AS entity, t.provenance
						FROM TimeSeries@{FORCE_INDEX=TimeSeriesByEntity3} AS t
						WHERE t.entity3 IN ('country/USA','country/IND')
							AND t.entity3 IS NOT NULL
							AND t.entity2 IS NOT NULL
					) AS ts
					GROUP BY ts.variable_measured
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