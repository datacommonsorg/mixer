		GRAPH DCGraph MATCH ANY (a0:Node)-[:Edge {predicate: 'typeOf'}]->(o0:Node
		WHERE
			o0.subject_id IN ('State')),
		(a0:Node
		WHERE
			a0.subject_id IN ('geoId/06')),
		(a0:Node)-[:Edge {predicate: 'name'}]->(a1:Node)
		RETURN
			a0.value AS a0,
			a1.value AS a1