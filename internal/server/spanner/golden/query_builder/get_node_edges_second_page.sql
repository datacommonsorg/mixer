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
        NEXT MATCH (n)
        WHERE
          n.subject_id = object_id
        RETURN
            subject_id,
            predicate,
            provenance,
            IFNULL(n.value, '') AS value,
            n.bytes,
            IFNULL(n.name, '') AS name,
            IFNULL(n.types, []) AS types
        ORDER BY
            subject_id,
            predicate,
            n.subject_id,
            provenance