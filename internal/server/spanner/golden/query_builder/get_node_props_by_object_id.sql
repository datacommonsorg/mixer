		GRAPH DCGraph MATCH -[e:Edge
		WHERE
			e.object_id IN UNNEST(@ids)]->
		RETURN DISTINCT
			e.object_id AS subject_id,
			e.predicate
		ORDER BY
			subject_id,
			predicate