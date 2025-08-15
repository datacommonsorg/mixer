		GRAPH DCGraph MATCH ANY (m:Node
		WHERE
			m.subject_id IN UNNEST(@ids))<-[e:Edge
		WHERE
			e.predicate = @predicate]-{1,10}(n:Node)
		RETURN
			m.subject_id,
			@result_predicate AS predicate,
			'' AS provenance,
			n.value,
			n.bytes,
			n.name,
			n.types
		ORDER BY
			subject_id,
			value
		LIMIT 5001