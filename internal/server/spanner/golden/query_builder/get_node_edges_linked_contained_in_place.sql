		GRAPH DCGraph MATCH <-[filter0:Edge
		WHERE
			filter0.predicate = 'typeOf'
			AND filter0.object_id IN ('County','County:E2yH3sRpXO/vAw/W3Hwy+utigKeV/acLAXGXtg47eHM=')]-(n),
		(m:Node
		WHERE
			m.subject_id IN ('country/USA'))<-[e:Edge
		WHERE
			e.predicate IN ('linkedContainedInPlace')]-(n:Node)
		RETURN
		  	m.subject_id,
			n.subject_id AS object_id,
			e.predicate,
			e.provenance
		NEXT MATCH (n)
		WHERE
		  n.subject_id = object_id
		RETURN
		  	subject_id,
			predicate,
			provenance,
			n.value,
			n.bytes,
			n.name,
			n.types
		ORDER BY
			subject_id,
			predicate,
			object_id,
			provenance
		LIMIT 501