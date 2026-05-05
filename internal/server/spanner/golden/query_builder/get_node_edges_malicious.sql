		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id = 'foo OR 1=1;')<-[e:Edge
		WHERE
			e.predicate = 'foo OR 1=1;']-(n:Node),
		(n)-[filter0:Edge
		WHERE
			filter0.predicate = 'foo OR 1=1;'
			AND filter0.object_id IN ('foo OR 1=1;','foo OR 1=1;:OG7012T2qe10jzYRBvG6dgUEx5fj7uIxT+RkGvxpn/U=')]->
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
			ANY_VALUE(n.value) AS value,
			ANY_VALUE(n.bytes) AS bytes,
			ANY_VALUE(n.name) AS name,
			ANY_VALUE(n.types) AS types
		GROUP BY
			subject_id,
			predicate,
			object_id,
			provenance
		ORDER BY
			subject_id,
			predicate,
			object_id,
			provenance
		LIMIT 501