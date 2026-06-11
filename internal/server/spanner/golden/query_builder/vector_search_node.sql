		SELECT
			subject_id,
			JSON_VALUE(embedding_content.description) AS name,
			node_types AS types,
			1 - COSINE_DISTANCE([0.1,0.2,0.3], embeddings) AS cosine_similarity
		FROM
			`NodeEmbedding`
		WHERE
			embeddings IS NOT NULL
			AND COSINE_DISTANCE([0.1,0.2,0.3], embeddings) <= 1 - 0.60
			AND EXISTS (
				SELECT 1 FROM UNNEST(node_types) AS t WHERE t IN ('StatisticalVariable','Topic')
			)
		ORDER BY
			APPROX_COSINE_DISTANCE([0.1,0.2,0.3], embeddings, options => JSON '{"num_leaves_to_search": 20}')
		LIMIT 5