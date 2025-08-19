		GRAPH DCGraph MATCH (n:Node
		WHERE
			n.subject_id IN ('country/USA','undata-geo:G00003340','Count_Person','foo'))
		RETURN
			n.subject_id AS node,
			n.subject_id AS candidate
	