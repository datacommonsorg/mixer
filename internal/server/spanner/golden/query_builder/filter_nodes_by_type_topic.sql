		SELECT subject_id, ARRAY(SELECT t FROM UNNEST(types) t WHERE t IN ('Topic')) AS matched_types
		FROM Node
		WHERE subject_id IN ('dc/g/Demographics','dc/topic/Demographics','WHO/Root')
			AND EXISTS (SELECT 1 FROM UNNEST(types) t WHERE t IN ('Topic'))