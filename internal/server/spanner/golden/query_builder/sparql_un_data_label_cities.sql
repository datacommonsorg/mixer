		GRAPH DCGraph MATCH ANY (subject_:Node)-[:Edge {predicate: 'unDataLabel'}]->(object_:Node),
		(subject_:Node)-[:Edge {predicate: 'typeOf'}]->(o1:Node
		WHERE
			o1.subject_id IN ('City'))
		RETURN DISTINCT 
			subject_.value
		LIMIT 10