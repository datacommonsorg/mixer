		SELECT
			n.subject_id,
			IFNULL(n.name, '') AS name,
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
					SELECT ts.variable_measured
					FROM (
						SELECT t.variable_measured, t.entity1 AS entity, t.provenance
						FROM TimeSeries AS t
						WHERE t.entity1 IN ('country/USA')
						UNION ALL
						SELECT t.variable_measured, t.entity2 AS entity, t.provenance
						FROM TimeSeries@{FORCE_INDEX=TimeSeriesByEntity2} AS t
						WHERE t.entity2 IN ('country/USA')
							AND t.entity2 IS NOT NULL
						UNION ALL
						SELECT t.variable_measured, t.entity3 AS entity, t.provenance
						FROM TimeSeries@{FORCE_INDEX=TimeSeriesByEntity3} AS t
						WHERE t.entity3 IN ('country/USA')
							AND t.entity3 IS NOT NULL
							AND t.entity2 IS NOT NULL
					) AS ts
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