		SELECT
			subject_id,
			embedding_content AS name,
			types,
			1 - COSINE_DISTANCE([0.1,0.2,0.3], embeddings) AS cosine_similarity
		FROM
			`NodeEmbedding`
		WHERE
			embeddings IS NOT NULL
			AND APPROX_COSINE_DISTANCE([0.1,0.2,0.3], embeddings, options => JSON '{"num_leaves_to_search": 20}') > 0.60
			AND EXISTS (
				SELECT 1 FROM UNNEST(types) AS t WHERE t IN ('StatisticalVariable','Topic')
			)
		ORDER BY
			APPROX_COSINE_DISTANCE([0.1,0.2,0.3], embeddings, options => JSON '{"num_leaves_to_search": 20}')
		LIMIT 5