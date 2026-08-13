		@{SCAN_METHOD=COLUMNAR}
		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id IN ('country/USA','country/IND'))<-[e:Edge
		WHERE
			e.predicate = 'linkedContainedInPlace']-(n:Node),
		@{FORCE_JOIN_ORDER=TRUE}
		(n)-[@{FORCE_INDEX=InEdge}filter0:Edge
		WHERE
			filter0.predicate = 'typeOf'
			AND filter0.object_id = 'Place']->
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
        NEXT LET dest = (
			SELECT AS STRUCT
				TRUE AS resolved,
				n.value,
				n.bytes,
				n.name,
				n.types,
			FROM Node n
			WHERE n.subject_id = object_id
		)
        RETURN
            subject_id,
            predicate,
			object_id,
            provenance,
			IFNULL(dest.resolved, FALSE) AS resolved,
            IFNULL(dest.value, '') AS value,
            dest.bytes,
            IFNULL(dest.name, '') AS name,
            IFNULL(dest.types, []) AS types
        ORDER BY
            subject_id,
            predicate,
            object_id,
            provenance