SELECT DISTINCT JSON_VALUE(t.facet, '$.measurementMethod') AS value
FROM TimeSeries t
WHERE t.variable_measured IN ('Count_Person','Count_Household')
  AND JSON_VALUE(t.facet, '$.measurementMethod') IS NOT NULL
  AND JSON_VALUE(t.facet, '$.measurementMethod') != ""
ORDER BY value
