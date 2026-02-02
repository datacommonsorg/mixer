		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id IN ('nuts/UKI1'))-[e:Edge]->(n:Node),
		<-[filter0:Edge
		WHERE
			filter0.predicate = 'description']-(n),
		<-[filter1:Edge
		WHERE
			filter1.predicate = 'subClassOf'
			AND filter1.object_id IN ('AdministrativeArea')]-(n)
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