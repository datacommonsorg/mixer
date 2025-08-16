		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id IN ('Farm'))<-[e:Edge]-(n:Node)
		,(n)-[filter0:Edge
		WHERE
			filter0.predicate = 'farmInventoryType'
			AND filter0.object_id IN ('Melon','mxuMmhySOejKGXRXFbMXdorKlNV934EOop6b21kOJGw=')]->
		,(n)-[filter1:Edge
		WHERE
			filter1.predicate = 'name'
			AND filter1.object_id IN ('Area of Farm: Melon','xblU8pfFl5m+cg9tsR1EsW19+PLlpqfNhwYkFu0mgzE=')]->
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