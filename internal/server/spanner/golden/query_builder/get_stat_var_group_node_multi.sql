		WITH ChildSVGs AS (
			SELECT DISTINCT
				subject_id AS child_svg, 
				object_id AS svg
			FROM Edge
			WHERE predicate = 'specializationOf'
			AND object_id IN ('dc/g/Demographics','dc/g/Economy')
			UNION ALL
			SELECT
				node AS child_svg,
				node AS svg
				FROM UNNEST(['dc/g/Demographics','dc/g/Economy']) AS node
		),
		UniqueChildSVGs AS (
			SELECT DISTINCT child_svg FROM ChildSVGs
		),
		ChildSVGCounts AS (
			SELECT 
				e.object_id AS child_svg, 
				COUNT(e.subject_id) AS descendent_stat_vars
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
			AND object_id IN ('dc/g/Demographics','dc/g/Economy')
		),
		UniqueChildSVs AS (
			SELECT DISTINCT child_sv FROM ChildSVs
		)
		SELECT 
			svg.svg,
			n.subject_id, 
			n.name, 
			c.descendent_stat_vars,
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
	