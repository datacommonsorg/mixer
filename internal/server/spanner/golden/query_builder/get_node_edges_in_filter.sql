		GRAPH DCGraph MATCH <-[filter0:Edge
		WHERE
			filter0.predicate = 'farmInventoryType'
			AND filter0.object_id IN ('Melon')]-(n),
		<-[filter1:Edge
		WHERE
			filter1.predicate = 'typeOf'
			AND filter1.object_id IN ('StatisticalVariable')]-(n),
		(m:Node
		WHERE
			m.subject_id IN ('Farm'))<-[e:Edge]-(n:Node)
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