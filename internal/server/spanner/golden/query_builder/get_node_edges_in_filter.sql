		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id IN UNNEST(@ids))<-[e:Edge]-(n:Node)
		,(n)-[filter0:Edge
		WHERE
			filter0.predicate = @prop0
			AND filter0.object_id IN UNNEST(@val0)]->
		,(n)-[filter1:Edge
		WHERE
			filter1.predicate = @prop1
			AND filter1.object_id IN UNNEST(@val1)]->
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