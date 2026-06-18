# SDMX JSON Schema Validation

This package contains SDMX response formatters and JSON golden files. The
availability formatter emits SDMX Structure JSON 2.0.0, so validate availability
goldens against the official SDMX structure-message schema.

Official schema:

- Canonical: https://json.sdmx.org/2.0.0/sdmx-json-structure-schema.json
- Source: https://github.com/sdmx-twg/sdmx-json/blob/v2.0.0/structure-message/tools/schemas/2.0.0/sdmx-json-structure-schema.json

## Install AJV CLI

Use `ajv-cli@3`. Newer `ajv-cli@5` uses AJV v8 and rejects one regex in the
official SDMX 2.0.0 schema before validating any payloads.

```bash
npm install --prefix /private/tmp/sdmx-ajv-cli-v3 ajv-cli@3
```

## Download The Schema

```bash
curl -L \
  -o /private/tmp/sdmx-json-structure-schema-2.0.0.json \
  https://json.sdmx.org/2.0.0/sdmx-json-structure-schema.json
```

If you already have the SDMX repo checked out, you can use the local schema file
instead:

```bash
/path/to/sdmx-json/structure-message/tools/schemas/2.0.0/sdmx-json-structure-schema.json
```

## Validate Availability Goldens

Run from the Mixer repo root:

```bash
/private/tmp/sdmx-ajv-cli-v3/node_modules/.bin/ajv validate \
  -s /private/tmp/sdmx-json-structure-schema-2.0.0.json \
  -d 'internal/server/sdmx/json/v2/golden/availability_*.json'
```

Expected output:

```text
internal/server/sdmx/json/v2/golden/availability_multiple_components.json valid
internal/server/sdmx/json/v2/golden/availability_observation_about.json valid
internal/server/sdmx/json/v2/golden/availability_time_period_empty.json valid
```

## Notes

- Use the structure-message schema for Availability API responses.
- Do not validate availability responses against the SDMX data-message schema.
- Availability responses include a top-level `$schema` field pointing to the
  canonical schema URL. Do not emit `meta.schema` unless the response also
  includes the other SDMX-required `meta` fields.
- The formatter intentionally omits `values: []` for empty availability
  results. The schema requires non-empty `values` when that field is present.
