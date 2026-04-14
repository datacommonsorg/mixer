		WITH ChildSVGs AS (
			SELECT DISTINCT
				subject_id AS child_svg, 
				object_id AS svg
			FROM Edge
			WHERE predicate = 'specializationOf'
			AND object_id = 'dc/g/Demographics'
			UNION ALL
			SELECT 
				'dc/g/Demographics' AS child_svg,
				'dc/g/Demographics' AS svg
		),
		UniqueChildSVGs AS (
			SELECT DISTINCT child_svg FROM ChildSVGs
		),
		ChildSVGCounts AS (
			SELECT 
				e.object_id AS child_svg, 
				COUNT(e.subject_id) AS descendent_stat_var_count
			FROM UniqueChildSVGs u
			JOIN@{JOIN_METHOD=APPLY_JOIN} Edge e 
			ON e.object_id = u.child_svg
			WHERE e.predicate = 'linkedMemberOf' 
			GROUP BY e.object_id
		),
		ChildSVs AS (
			SELECT DISTINCT
				subject_id AS child_sv, 
				object_id AS svg
			FROM Edge
			WHERE predicate = 'memberOf'
			AND object_id = 'dc/g/Demographics'
		),
		UniqueChildSVs AS (
			SELECT DISTINCT child_sv FROM ChildSVs
		)
		SELECT 
			svg.svg,
			n.subject_id, 
			n.name, 
			c.descendent_stat_var_count,
			FALSE AS has_data
		FROM ChildSVGs svg
		JOIN ChildSVGCounts c 
		ON svg.child_svg = c.child_svg
		JOIN Node n 
		ON n.subject_id = svg.child_svg
		UNION ALL
		SELECT 
			sv.svg,
			n.subject_id, 
			n.name, 
			-1 AS descendent_stat_var_count,
			EXISTS (
				SELECT 1 
				FROM Observation o 
				WHERE o.variable_measured = sv.child_sv
				LIMIT 1
			) AS has_data
		FROM ChildSVs sv
		JOIN Node n 
		ON n.subject_id = sv.child_sv