		GRAPH DCGraph MATCH ANY (a0:Node)-[:Edge {predicate: 'typeOf'}]->(o0:Node
		WHERE
			o0.subject_id IN ('Country')),
		(a0:Node)-[:Edge {predicate: 'name'}]->(a1:Node)
		RETURN DISTINCT 
			a1.value AS a1
		ORDER BY 
			a1
		DESC
		LIMIT 10