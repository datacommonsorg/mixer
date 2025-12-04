		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id IN ('Person'))-[e:Edge
		WHERE
			e.predicate IN ('source')]->(n:Node)
		RETURN
			m.subject_id,
			e.predicate,
			e.provenance,
			n.value,
			n.bytes,
			n.name,
			n.types
		ORDER BY
			subject_id,
			predicate,
			n.subject_id,
			provenance
		LIMIT 501