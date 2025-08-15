		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id IN UNNEST(@ids))<-[e:Edge
		WHERE
			e.predicate IN UNNEST(@predicates)]-(n:Node)
		,(n)-[filter0:Edge
		WHERE
			filter0.predicate = @prop0
			AND filter0.object_id IN UNNEST(@val0)]->
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