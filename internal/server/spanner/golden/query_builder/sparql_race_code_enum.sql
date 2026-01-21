		GRAPH DCGraph MATCH ANY (a_:Node)-[:Edge {predicate: 'typeOf'}]->(o0:Node
		WHERE
			o0.subject_id IN ('RaceCodeEnum'))
		RETURN
			a_.value AS a_
		ORDER BY 
			a_.value