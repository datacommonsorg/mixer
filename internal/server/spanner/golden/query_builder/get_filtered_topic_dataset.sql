		SELECT
			e.object_id AS subject_id,
			COUNT(DISTINCT e.subject_id) AS descendent_stat_var_count
		FROM Edge@{FORCE_INDEX=InEdge} e
		JOIN@{JOIN_TYPE=HASH_JOIN} (
					SELECT summary.key AS variable_measured
					FROM Edge@{FORCE_INDEX=InEdge} AS provenance_edge
					JOIN KeyValueStore@{FORCE_INDEX=KeyValueStoreByProvenance} AS summary
					ON summary.provenance = provenance_edge.subject_id
					WHERE provenance_edge.predicate = 'isPartOf'
						AND provenance_edge.object_id = 'dc/d/TestDataset'
						AND summary.type = 'ProvenanceSummary'
					GROUP BY summary.key
				) AS o
					ON o.variable_measured = e.subject_id
		WHERE e.predicate = 'linkedMember'
			AND e.object_id = 'dc/topic/Demographics'
		GROUP BY
			e.object_id