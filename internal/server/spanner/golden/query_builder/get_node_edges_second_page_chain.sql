		GRAPH DCGraph MATCH ANY (m:Node
		WHERE
			m.subject_id IN ('dc/g/UN'))<-[e:Edge
		WHERE
			e.predicate = 'specializationOf']-{1,10}(n:Node)
		RETURN DISTINCT
			m.subject_id,
			n.subject_id AS value
		NEXT MATCH (n)
		WHERE
		  n.subject_id = value
		RETURN
		  subject_id,
			'specializationOf+' AS predicate,
			'' AS provenance,
			n.value,
			n.bytes,
			n.name,
			n.types
		ORDER BY
			subject_id,
			value
		OFFSET 500
		LIMIT 501