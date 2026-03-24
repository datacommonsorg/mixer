		@{force_join_order=true}
		GRAPH DCGraph MATCH (event:Node)-[:Edge {predicate: 'affectedPlace', object_id: 'country/GBR'}]->(), (event)-[:Edge {predicate: 'typeOf', object_id: 'FloodEvent'}]->(), (event)-[:Edge {predicate: 'startDate'}]->(dateNode:Node)
		WHERE 
			SUBSTR(dateNode.value, 1, 7) = '2025-01'
		OPTIONAL MATCH (event)-[magEdge:Edge {predicate: 'area'}]->()
		RETURN DISTINCT 
			event.subject_id AS dcid,
			magEdge.object_id AS magnitude