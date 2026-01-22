		GRAPH DCGraph MATCH ANY (country_:Node)-[:Edge {predicate: 'typeOf'}]->(o0:Node
		WHERE
			o0.subject_id IN ('Country')),
		(country_:Node)-[:Edge {predicate: 'name'}]->(name_:Node)
		RETURN DISTINCT 
			name_.value AS name_
		ORDER BY 
			name_
		DESC
		LIMIT 10