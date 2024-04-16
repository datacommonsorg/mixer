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

## recon_name_to_types.json generation

This file is a map where the keys are reconNames that are common words like "me", "to", "a", etc., and values are the allowed types of the entities recognized for each reconName. If a reconName is in this map, the entity can only be recognized if the name is preceded by one of the allowed types in the query.
For example, there is a gene called "Me". The query "Tell me about the gene me" should not recognize the first "me", but should recognize the "gene me".

1. Use BQ to get the list of names/alternate names and their types for a list of entity types that we do name recon for. Query used for current file:

```sql
SELECT
  t1.object_value as name,
  t2.object_id
FROM `datcom-store.dc_kg_latest.Triple` as t1
JOIN `datcom-store.dc_kg_latest.Triple` as t2
ON t1.subject_id = t2.subject_id 
WHERE 
  t1.predicate = 'name' and
  t2.predicate = 'typeOf' and
  t2.object_id in ("VirusGenusEnum", "VirusIsolate", "Virus", "Species", "BiologicalSpecimen", "GeneticVariant", "Gene", "Disease", "ICD10Section", "ICD10Code", "MeSHDescriptor", "Drug", "AnatomicalTherapeuticChemicalCode", "MeSHSupplementaryRecord")
```

Note: the list of entity types that t2.object_id should match to should match the list [here](https://source.corp.google.com/piper///depot/google3/datacommons/import/mcf_vocab.h;l=459-464;rcl=625130528).

2. Put the results of (1) through a script that takes the names and uses spacy to get its "part of sentence" classification ([e.g., noun, pronoun, etc](https://melaniewalsh.github.io/Intro-Cultural-Analytics/05-Text-Analysis/13-POS-Keywords.html#spacy-part-of-speech-tagging)). The script then flags names that do not have any punctuation, nouns, proper nouns or numbers (potentially a common word). TODO: add link to script once it's checked in

3. Manually go through the flagged names from (2) and add actual common words to recon_name_to_types.json