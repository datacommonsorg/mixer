		GRAPH DCGraph MATCH ANY (state_:Node
		WHERE
			state_.subject_id IN ('geoId/06'))-[:Edge {predicate: 'typeOf'}]->(o0:Node
		WHERE
			o0.subject_id IN ('State')),
		(state_:Node)-[:Edge {predicate: 'name'}]->(name_:Node)
		RETURN
			name_.value