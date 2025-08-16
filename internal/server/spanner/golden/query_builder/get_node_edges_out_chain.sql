		GRAPH DCGraph MATCH ANY (m:Node
		WHERE
			m.subject_id IN ('dc/g/Person_Gender'))-[e:Edge
		WHERE
			e.predicate = 'specializationOf']->{1,10}(n:Node)
		RETURN
			m.subject_id,
			'specializationOf+' AS predicate,
			'' AS provenance,
			n.value,
			n.bytes,
			n.name,
			n.types
		ORDER BY
			subject_id,
			value
		LIMIT 5001