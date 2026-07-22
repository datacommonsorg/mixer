		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id IN ('country/USA','country/IND'))<-[@{FORCE_INDEX=InEdge}e:Edge
		WHERE
			e.predicate = 'linkedContainedInPlace']-(n:Node),
		@{FORCE_JOIN_ORDER=TRUE}
		(n)-[@{FORCE_INDEX=_BASE_TABLE}filter0:Edge
		WHERE
			filter0.predicate = 'typeOf'
			AND filter0.object_id IN ('Place','Place:6UY9zPBUGpd4WJaRVD/Wkkl1nV3mqXzVfWQgw+XZK6o=')]->
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