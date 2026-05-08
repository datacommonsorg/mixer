		SELECT DISTINCT subject_id
		FROM Node
		WHERE ARRAY_LENGTH(types) > 0
			AND 'Topic' IN UNNEST(types)
			AND subject_id IN ('dc/g/Demographics','dc/topic/Demographics','WHO/Root')