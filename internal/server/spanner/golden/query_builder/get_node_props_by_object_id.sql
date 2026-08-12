		@{SCAN_METHOD=COLUMNAR}
		GRAPH DCGraph MATCH (m:Node
		WHERE m.subject_id IN ('Count_Person','Person'))<-[e:Edge
		WHERE
			e.predicate NOT IN ('linkedContainedInPlace','linkedMemberOf','linkedMember')]-
		RETURN DISTINCT
			m.subject_id,
			e.predicate
		ORDER BY
			subject_id,
			predicate