# SDMX Server Code

This tree implements Mixer's SDMX endpoints under `/sdmx/v3/...`.

## Packages

- `transport/grpc`: adapts grpc-gateway metadata into `service.Request`.
- `service`: runs endpoint workflows and returns transport-neutral responses.
- `rest/v2`: parses and validates SDMX REST v2 paths, queries, and media negotiation.
- `format`: owns response content types and serialization packages.
- `datacommons`: Data Commons-supported SDMX dataflows, component IDs, defaults, and mappings shared by REST, service, and format code.

## Dependency Shape

`handler_sdmx_v3` calls `transport/grpc` and `service`.
`service` calls `rest/v2`, `format/*`, and dispatcher code.
`rest/v2` and `format/*` may import `datacommons`.
`transport/grpc` does not parse SDMX REST syntax.

## Guidelines

- Shared constants should live with the concept they describe.
- Use `datacommons` only for Data Commons-supported SDMX dataflows, component IDs, defaults, and mappings used by multiple layers.
- Keep REST syntax in `rest/v2` and response media constants in `format`.

// TODO(rohitrkumar): Polish it