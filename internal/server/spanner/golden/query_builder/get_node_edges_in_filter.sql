		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id = 'Farm')<-[e:Edge]-(n:Node),
		(n)-[@{FORCE_INDEX=InEdge}filter0:Edge
		WHERE
			filter0.predicate = 'farmInventoryType'
			AND filter0.object_id IN ('Melon','Melon:mxuMmhySOejKGXRXFbMXdorKlNV934EOop6b21kOJGw=')]->,
		(n)-[@{FORCE_INDEX=InEdge}filter1:Edge
		WHERE
			filter1.predicate = 'name'
			AND filter1.object_id IN ('Area of Farm: Melon','Area of Farm: Me:xblU8pfFl5m+cg9tsR1EsW19+PLlpqfNhwYkFu0mgzE=')]->
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
			IFNULL(ANY_VALUE(n.value), '') AS value,
			ANY_VALUE(n.bytes) AS bytes,
			IFNULL(ANY_VALUE(n.name), '') AS name,
			IFNULL(ANY_VALUE(n.types), []) AS types
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