		@{force_join_order=true}
		GRAPH DCGraph MATCH (event:Node)-[:Edge {predicate: 'affectedPlace', object_id: 'country/GBR'}]->(), (event)-[:Edge {predicate: 'typeOf', object_id: 'FloodEvent'}]->(), (event)-[:Edge {predicate: 'startDate'}]->(dateNode:Node)
		OPTIONAL MATCH (event)-[:Edge {predicate: 'area'}]->(magNode:Node)
		WHERE 
			SUBSTR(dateNode.value, 1, 7) = '2025-01'
		RETURN DISTINCT 
			event.subject_id AS dcid,
			magNode.value AS magnitude