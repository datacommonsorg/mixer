## BQ SQL query for WorldGeosForPlaceRecognitionAbbreviatedNames.csv

TODO(ws|pradh): We could also expand to isoCode for AA1's which has codes like "US-CA" from which we parse out the last two chars, so TN (for Tamil Nadu state), SH (for shanghai). But we might prefer US states if we do that.

```sql
SELECT
  id,
  RTRIM(CONCAT(IFNULL(CONCAT(country_alpha_2_code, ","), ""), IFNULL(CONCAT(country_alpha_3_code, ","), "")), ",") AS abbreviatedNames
FROM
  `datcom-store.dc_kg_latest.Place`
WHERE
  country_alpha_2_code != ""
  OR country_alpha_3_code != ""
UNION ALL
SELECT
  subject_id AS id,
  object_value AS fips52AlphaCode
FROM
  `datcom-store.dc_kg_latest.Triple`
WHERE
  predicate = 'fips52AlphaCode'
  AND subject_id LIKE 'geoId/%'
```
