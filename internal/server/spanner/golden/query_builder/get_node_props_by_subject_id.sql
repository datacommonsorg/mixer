		GRAPH DCGraph MATCH -[e:Edge
		WHERE
			e.subject_id IN ('Count_Person','Person','foo')]->
		RETURN DISTINCT
			e.subject_id,
			e.predicate
		ORDER BY
			e.subject_id,
			e.predicate