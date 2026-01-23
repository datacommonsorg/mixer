		GRAPH DCGraph MATCH ANY (a0:Node)-[:Edge {predicate: 'unDataLabel'}]->(a1:Node),
		(a0:Node)-[:Edge {predicate: 'typeOf'}]->(o1:Node
		WHERE
			o1.subject_id IN ('City'))
		RETURN DISTINCT 
			a0.value AS a0
		LIMIT 10