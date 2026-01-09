		GRAPH DCGraph MATCH (n:Node)
		WHERE
			SEARCH(n.name_tokenlist, 'income')
		AND ARRAY_INCLUDES_ANY(n.types, ['StatisticalVariable'])
		RETURN
			n.subject_id, 
			n.name,
			n.types, 
			SCORE(n.name_tokenlist, 'income', enhance_query => TRUE) AS score 
		ORDER BY 
			score + IF(n.name = 'income', 1, 0) DESC,
			n.name ASC
		LIMIT 100