		GRAPH DCGraph MATCH (n:Node)
		WHERE n.name_embeddings IS NOT NULL
			AND APPROX_COSINE_DISTANCE([0.1,0.2,0.3], n.name_embeddings, options => JSON '{"num_leaves_to_search": 20}') > 0.6
		RETURN
			n.subject_id,
			n.name, 
			1 - COSINE_DISTANCE([0.1,0.2,0.3], n.name_embeddings) AS cosine_similarity
		ORDER BY APPROX_COSINE_DISTANCE([0.1,0.2,0.3], n.name_embeddings, options => JSON '{"num_leaves_to_search": 20}')
		LIMIT 5