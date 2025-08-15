
		GRAPH DCGraph MATCH (n:Node)
		WHERE
			SEARCH(n.name_tokenlist, @query)
		AND ARRAY_INCLUDES_ANY(n.types, @types)
		RETURN
			n.subject_id, 
			n.name,
			n.types, 
			SCORE(n.name_tokenlist, @query, enhance_query => TRUE) AS score 
		ORDER BY 
			score + IF(n.name = @query, 1, 0) DESC,
			n.name ASC
		LIMIT 100