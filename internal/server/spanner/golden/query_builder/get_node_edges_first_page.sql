		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id = 'StatisticalVariable')<-[e:Edge
		WHERE
			e.predicate = 'typeOf']-(n:Node)
		RETURN
			m.subject_id,
			e.predicate,
			e.provenance,
			IFNULL(n.value, '') AS value,
			n.bytes,
			IFNULL(n.name, '') AS name,
			IFNULL(n.types, []) AS types
		ORDER BY
			subject_id,
			predicate,
			n.subject_id,
			provenance
		LIMIT 501