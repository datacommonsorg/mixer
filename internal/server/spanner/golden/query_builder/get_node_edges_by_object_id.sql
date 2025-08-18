		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id IN ('FireIncidentTypeEnum','FoodTypeEnum'))<-[e:Edge]-(n:Node)
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