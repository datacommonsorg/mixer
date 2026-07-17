		SELECT embeddings.values
		FROM ML.PREDICT(MODEL `test_model`, (SELECT 'test_query' AS content, 'RETRIEVAL_QUERY' AS task_type))