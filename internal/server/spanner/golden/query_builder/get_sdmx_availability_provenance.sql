SELECT DISTINCT t.provenance AS value
FROM TimeSeries t
WHERE t.variable_measured IN ('Count_Person','Count_Household')
  AND t.provenance IS NOT NULL
  AND t.provenance != ""
ORDER BY value
