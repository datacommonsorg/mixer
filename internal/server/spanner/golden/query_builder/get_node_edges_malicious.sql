		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id IN ('foo OR 1=1;'))<-[e:Edge
		WHERE
			e.predicate IN ('foo OR 1=1;')]-(n:Node)
		,(n)-[filter0:Edge
		WHERE
			filter0.predicate = 'foo OR 1=1;'
			AND filter0.object_id IN ('foo OR 1=1;','OG7012T2qe10jzYRBvG6dgUEx5fj7uIxT+RkGvxpn/U=')]->
		RETURN
		  m.subject_id,
			n.subject_id AS value,
			e.predicate,
			e.provenance
		NEXT MATCH (n)
		WHERE
		  n.subject_id = value
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
			value,
			provenance
		LIMIT 5001