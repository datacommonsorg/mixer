# SDMX Server Code

This tree implements Mixer's SDMX endpoints under `/sdmx/...`.

## Coding Guidelines

### Where Code Belongs

- **`transport/grpc`**: Extract gRPC metadata and HTTP headers (such as `x-dc-original-uri`, `x-envoy-original-path`, and `accept`) into `service.Request`. Do not parse SDMX REST paths, query strings, component filters, or response formats here.
- **`service`**: Orchestrate endpoint workflows. Parse through `rest/v2`, map parsed requests to dispatcher queries, call dispatcher, and call the selected formatter.
- **`rest/v2`**: Parse and validate SDMX REST v2 request syntax. Own path parsing, raw query parsing, component filters, supported query parameters, and response format selection helpers.
- **`format/*`**: Serialize already-fetched domain data into supported response payloads, such as JSON-stat, SDMX-CSV, and SDMX Structure JSON. Do not read HTTP headers, URLs, query params, or call dispatcher.
- **`datacommons`**: Hold Data Commons-specific SDMX defaults, component metadata, and mappings shared by multiple packages. Do not put endpoint-local parser constants here.

### Import Direction

Allowed imports:

- Entry points may import `transport/grpc` and `service`.
- `transport/grpc` may import `service`.
- `service` may import `rest/v2`, `format/*`, `datacommons`, and internal dispatcher packages.
- `rest/v2` and `format/*` may import `datacommons`.
- `datacommons` must not import other `sdmx` packages.

Do not import upward from lower-level packages into `service`, `transport`, or entry-point packages.

### Placement Rule

- If code parses external SDMX request syntax, put it in `rest/v2`.
- If code decides workflow, maps to dispatcher requests, or calls dispatcher, put it in `service`.
- If code writes response-body bytes, put it in `format/*`.
- If code names Data Commons-supported SDMX concepts shared across packages, put it in `datacommons`.
- If code only extracts gRPC/HTTP metadata, put it in `transport/grpc`.
