		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id IN ('geoId/5129600'))-[e:Edge
		WHERE
			e.predicate IN ('containedInPlace','geoJsonCoordinatesDP3')]->(n:Node)
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
			value,
			provenance
		LIMIT 5001