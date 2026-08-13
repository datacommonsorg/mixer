		@{SCAN_METHOD=COLUMNAR}
		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id = 'foo OR 1=1;')<-[e:Edge
		WHERE
			e.predicate = 'foo OR 1=1;']-(n:Node),
		(n)-[@{FORCE_INDEX=InEdge}filter0:Edge
		WHERE
			filter0.predicate = 'foo OR 1=1;'
			AND filter0.object_id IN ('foo OR 1=1;','foo OR 1=1;:OG7012T2qe10jzYRBvG6dgUEx5fj7uIxT+RkGvxpn/U=')]->
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
            IFNULL(dest.value, '') AS value,
            dest.bytes,
            IFNULL(dest.name, '') AS name,
            IFNULL(dest.types, []) AS types
        ORDER BY
            subject_id,
            predicate,
            object_id,
            provenance