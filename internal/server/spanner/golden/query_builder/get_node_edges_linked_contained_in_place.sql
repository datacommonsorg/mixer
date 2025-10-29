		GRAPH DCGraph MATCH ANY <-[filter0:Edge
		WHERE
			filter0.predicate = 'typeOf'
			AND filter0.object_id IN ('County','E2yH3sRpXO/vAw/W3Hwy+utigKeV/acLAXGXtg47eHM=')]-(n),
		(m:Node
		WHERE
			m.subject_id IN ('country/USA'))<-[e:Edge
		WHERE
			e.predicate = 'containedInPlace']-{1,10}(n:Node)
		RETURN DISTINCT
			m.subject_id,
			n.subject_id AS value
		NEXT MATCH (n)
		WHERE
		  n.subject_id = value
		RETURN
		  subject_id,
			'containedInPlace+' AS predicate,
			'' AS provenance,
			n.value,
			n.bytes,
			n.name,
			n.types
		ORDER BY
			subject_id,
			value
		OFFSET 5000
		LIMIT 5001