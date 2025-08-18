		GRAPH DCGraph MATCH -[e:Edge
		WHERE
			e.object_id IN ('Count_Person','Person')]->
		RETURN DISTINCT
			e.object_id AS subject_id,
			e.predicate
		ORDER BY
			subject_id,
			predicate