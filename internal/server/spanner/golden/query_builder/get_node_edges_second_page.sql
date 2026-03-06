		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id = 'StatisticalVariable')<-[e:Edge
		WHERE
			e.predicate = 'typeOf']-(n:Node)
		RETURN
			m.subject_id,
			e.predicate,
			e.provenance,
			n.value,
			n.bytes,
			n.name,
			n.types
		ORDER BY
			subject_id,
			predicate,
			n.subject_id,
			provenance
		OFFSET 500
		LIMIT 501