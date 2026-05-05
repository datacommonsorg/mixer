		@{force_join_order=true}
		GRAPH DCGraph
		MATCH (event:Node)-[:Edge {predicate: 'typeOf', object_id: 'FireEvent'}]->()
		WITH DISTINCT event
		MATCH (event:Node)-[:Edge {predicate: 'affectedPlace', object_id: 'country/LBR'}]->()
		MATCH (event:Node)-[:Edge {predicate: 'startDate'}]->(dateNode:Node)
		WHERE 
			SUBSTR(dateNode.value, 1, 7) = '2020-10'
		MATCH (event:Node)-[magEdge:Edge {predicate: 'area'}]->()
		RETURN DISTINCT 
			event.subject_id AS dcid,
			magEdge.object_id AS magnitude