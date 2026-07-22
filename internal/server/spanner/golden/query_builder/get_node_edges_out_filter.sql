		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id = 'nuts/UKI1')-[e:Edge]->(n:Node),
		(n)-[@{FORCE_INDEX=InEdge}filter0:Edge
		WHERE
			filter0.predicate = 'name'
			AND filter0.object_id IN ('AdministrativeArea2','AdministrativeAr:4cB0ui47vrAeY7MO/uBAvpsajxkYlJo3EW8fStdW4ko=')]->,
		(n)-[@{FORCE_INDEX=InEdge}filter1:Edge
		WHERE
			filter1.predicate = 'subClassOf'
			AND filter1.object_id IN ('AdministrativeArea','AdministrativeAr:WXALAhw8j+Uz/Tw7uR3ClTolVepyj0tjRCKr6Xkw60s=')]->
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