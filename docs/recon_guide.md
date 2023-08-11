# Data Commons Reconciliation Usage Guide

In the following example query, replace `<YOUR-API-KEY>` with your DC API key.

The results are stored in the `candidates` field in the API response.

## Resolve description

Given a description of an entity, return the DCIDs of possible candidates. Usually the entity is a place, and the description is the name of the place. The name could be: "Mountain View", "Mountain View, CA", etc.

```bash
curl -X POST \
--url https://api.datacommons.org/v2/resolve \
--header 'X-API-Key: <YOUR-API-KEY>' \
--data '{
  "nodes": [
    "Mountain View",
    "Sunnyvale",
  ],
  "property": "<-description->dcid",
}'
```

## Resolve place coordinate

Given a place cooridnate (latitude, longitude), return the DCIDs of places that contain the coordinate.

NOTE: The format of the coordinate must be `<latitude>#<longitude>`, see the example query below.

```bash
curl -X POST \
--url https://api.datacommons.org/v2/resolve \
--header 'X-API-Key: <YOUR-API-KEY>' \
--data '{
  "nodes": [
    "37.42#-122.14",
    "40.42#-123.14",
  ],
  "property": "<-geoCoordinate->dcid",
}'
```

## Resolve ID

Given a kind of ID of an entity, return a list of other kind of IDs of candidates.

```bash
curl -X POST \
--url https://api.datacommons.org/v2/resolve \
--header 'X-API-Key: <YOUR-API-KEY>' \
--data '{
  "nodes": [
    "Q486860",
    "Q16559",
  ],
  "property": "<-wikidataId->dcid",
}'
```

## Recognize places

Given a sentence, find all the place entities in it.

```bash
curl -X POST \
--url https://api.datacommons.org/v1/recognize/places \
--header 'X-API-Key: <YOUR-API-KEY>' \
--data '{
  "queries": [
    "I went to Mountain View, CA for work.",
    "Boston and Houston are two great places",
  ],
}'
```