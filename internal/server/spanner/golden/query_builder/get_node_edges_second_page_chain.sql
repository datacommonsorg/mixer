		GRAPH DCGraph MATCH ANY (m:Node
		WHERE
			m.subject_id = 'dc/g/UN')<-[e:Edge
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
			IFNULL(n.value, '') AS value,
			n.bytes,
			IFNULL(n.name, '') AS name,
			IFNULL(n.types, []) AS types
		ORDER BY
			subject_id,
			object_id
		OFFSET 500
		LIMIT 501