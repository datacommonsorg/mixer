		GRAPH DCGraph MATCH ANY (a0:Node)-[:Edge {predicate: 'typeOf'}]->(o0:Node
		WHERE
			o0.subject_id IN ('RaceCodeEnum'))
		RETURN
			a0.value AS a0
		ORDER BY 
			a0