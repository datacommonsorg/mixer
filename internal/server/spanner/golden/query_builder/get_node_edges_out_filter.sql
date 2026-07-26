		@{SCAN_METHOD=COLUMNAR}
		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id = 'nuts/UKI1')-[e:Edge
		WHERE
			e.predicate NOT IN ('linkedContainedInPlace','linkedMemberOf','linkedMember')]->(n:Node),
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
            e.predicate,
            n.subject_id AS object_id,
            e.provenance
		GROUP BY
            m.subject_id,
            e.predicate,
            n.subject_id,
            e.provenance
        ORDER BY
            subject_id,
            predicate,
            object_id,
            provenance
		LIMIT 501
        NEXT MATCH (n:Node)
        WHERE
            n.subject_id = object_id
        RETURN
            subject_id,
            predicate,
            provenance,
            IFNULL(n.value, '') AS value,
            n.bytes AS bytes,
            IFNULL(n.name, '') AS name,
            IFNULL(n.types, []) AS types
        ORDER BY
            subject_id,
            predicate,
            object_id,
            provenance