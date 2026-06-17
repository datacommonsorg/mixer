SELECT DISTINCT JSON_VALUE(t.facet, '$.unit') AS value
FROM TimeSeries t
WHERE t.variable_measured IN ('Count_Person','Count_Household')
  AND JSON_VALUE(t.facet, '$.unit') IS NOT NULL
  AND JSON_VALUE(t.facet, '$.unit') != ""
ORDER BY value
