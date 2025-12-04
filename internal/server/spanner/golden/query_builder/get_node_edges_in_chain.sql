		GRAPH DCGraph MATCH ANY (m:Node
		WHERE
			m.subject_id IN ('dc/g/Farm_FarmInventoryStatus'))<-[e:Edge
		WHERE
			e.predicate = 'specializationOf']-{1,10}(n:Node)
		RETURN DISTINCT
			m.subject_id,
			n.subject_id AS object_id
		NEXT MATCH (n)
		WHERE
		  n.subject_id = object_id
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
			object_id
		LIMIT 501