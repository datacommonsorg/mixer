		GRAPH DCGraph MATCH -[e:Edge
		WHERE
			e.subject_id IN UNNEST(@ids)]->
		RETURN DISTINCT
			e.subject_id,
			e.predicate
		ORDER BY
			e.subject_id,
			e.predicate