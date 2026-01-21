		GRAPH DCGraph MATCH ANY (biologicalSpecimen_:Node)-[:Edge {predicate: 'typeOf'}]->(o0:Node
		WHERE
			o0.subject_id IN ('BiologicalSpecimen')),
		(biologicalSpecimen_:Node)-[:Edge {predicate: 'name'}]->(name_:Node)
		RETURN DISTINCT 
			name_.value AS name_
		ORDER BY 
			name_.value
		DESC
		LIMIT 10