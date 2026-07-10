		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id = 'geoId/5129600')-[e:Edge
		WHERE
			e.predicate IN ('containedInPlace','geoJsonCoordinatesDP3')]->(n:Node)
		RETURN
			m.subject_id,
			e.predicate,
			e.provenance,
			IFNULL(n.value, '') AS value,
			n.bytes,
			IFNULL(n.name, '') AS name,
			IFNULL(n.types, []) AS types
		ORDER BY
			subject_id,
			predicate,
			n.subject_id,
			provenance
		LIMIT 501