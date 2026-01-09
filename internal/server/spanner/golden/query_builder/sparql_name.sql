		GRAPH DCGraph MATCH ANY (s0:Node
		WHERE
			s0.subject_id IN ('geoId/06'))-[:Edge {predicate: 'typeOf'}]->(o0:Node
		WHERE
			o0.subject_id IN ('State')),
		(s1:Node
		WHERE
			s1.subject_id IN ('geoId/06'))-[:Edge {predicate: 'name'}]->(name_:Node)
		RETURN
			name_.value