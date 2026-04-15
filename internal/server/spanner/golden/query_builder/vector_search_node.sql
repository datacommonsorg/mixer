		SELECT
			subject_id,
			embedding_content,
			1 - COSINE_DISTANCE([0.1,0.2,0.3], embeddings) AS cosine_similarity
		FROM
			NodeEmbeddings
		WHERE
			embeddings IS NOT NULL
			AND APPROX_COSINE_DISTANCE([0.1,0.2,0.3], embeddings, options => JSON '{"num_leaves_to_search": 20}') > 0.6
		ORDER BY
			APPROX_COSINE_DISTANCE([0.1,0.2,0.3], embeddings, options => JSON '{"num_leaves_to_search": 20}')
		LIMIT 5