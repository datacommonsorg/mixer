		SELECT
			n.subject_id,
			IFNULL(n.name, '') AS name,
			'' AS definition
		FROM Node n
		JOIN (
			SELECT
				e.subject_id AS subject_id
			FROM Edge e
			JOIN@{JOIN_TYPE=HASH_JOIN} (
					SELECT summary.key AS variable_measured
					FROM Edge@{FORCE_INDEX=InEdge} AS provenance_edge
					JOIN KeyValueStore@{FORCE_INDEX=KeyValueStoreByProvenance} AS summary
					ON summary.provenance = provenance_edge.subject_id
					WHERE provenance_edge.predicate = 'source'
						AND provenance_edge.object_id = 'dc/s/WorldBank'
						AND summary.type = 'ProvenanceSummary'
					GROUP BY summary.key
				) AS o
					ON o.variable_measured = e.subject_id
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