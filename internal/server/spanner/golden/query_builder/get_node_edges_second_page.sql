		@{SCAN_METHOD=COLUMNAR}
		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id = 'StatisticalVariable')<-[e:Edge
		WHERE
			e.predicate = 'typeOf']-(n:Node)  
        RETURN
            m.subject_id,
            e.predicate,
            n.subject_id as object_id,
            e.provenance
        ORDER BY
            subject_id, 
            predicate,
            object_id,
            provenance
		OFFSET 500
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