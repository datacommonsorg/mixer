# SDMX HTTP Responses Over gRPC

## Decision

Keep the existing HTTP-to-gRPC path for the SDMX 3 APIs:

```text
HTTP client
  -> local Envoy or GKE ESPv2
  -> Mixer gRPC method
  -> google.api.HttpBody
  -> raw HTTP JSON or CSV response
```

Prefer this design over adding a second Mixer HTTP listener, provided an
integration spike confirms that Envoy and ESPv2 preserve the original request
URI in incoming gRPC metadata.

This preserves the existing ESPv2 API key checks, quota handling, telemetry,
and single-backend deployment model. Mixer still receives a typed gRPC request,
but the successful response is an arbitrary byte payload with an explicit HTTP
content type.

The response does not need a protobuf message for every SDMX-JSON field.

## Why `HttpBody`

The existing SDMX data RPC returns:

```proto
message SdmxDataResponse {
  string payload = 1;
}
```

Through normal JSON transcoding, that produces a protobuf JSON wrapper around
an escaped string. It does not produce the raw SDMX document expected by an
HTTP client.

Envoy's gRPC-JSON transcoder has special handling for
`google.api.HttpBody`. For a successful response, it extracts the `data` bytes
as the HTTP response body and sets the HTTP `Content-Type` from
`content_type`.

For example, Mixer can return:

```go
&httpbody.HttpBody{
    ContentType: "application/vnd.sdmx.data+csv;version=2.0.0",
    Data:        csvBytes,
}
```

The HTTP client receives CSV bytes directly, not protobuf JSON.

The same mechanism works for a constructed SDMX-JSON document:

```go
&httpbody.HttpBody{
    ContentType: "application/vnd.sdmx.data+json;version=2.0.0",
    Data:        jsonBytes,
}
```

## Public GET Surface

The initial public routes should follow the SDMX REST shape under the Data
Commons `/sdmx/v3` prefix:

```text
GET /sdmx/v3/data/{context}/{agencyID}/{resourceID}/{version}/{key}
GET /sdmx/v3/availability/{context}/{agencyID}/{resourceID}/{version}/{key}/{componentID}
```

At the time this note was written, the upstream SDMX REST OpenAPI definition is
version `2.2.2` and describes the REST API for SDMX `3.1`. Pin the version
before implementation rather than tracking upstream `master` implicitly.

The exact optional path variants should be decided when pinning the supported
SDMX REST specification version. In protobuf HTTP annotations, optional
suffixes are represented by explicit `additional_bindings`; they are not
implicit.

The current experimental route is:

```text
/v3/sdmx/v3/data/**
```

It should be replaced with explicit path-bound request fields. This gives the
handler named values and lets the transcoder handle standard URL decoding.

## Request Protobufs

Only the request path needs ordinary protobuf fields. Bind the SDMX path
components explicitly, but parse the query string from the original request
URI inside Mixer.

An initial sketch:

```proto
import "google/api/annotations.proto";
import "google/api/httpbody.proto";

message SdmxDataRequest {
  string context = 1;
  string agency_id = 2;
  string resource_id = 3;
  string version = 4;
  string key = 5;

  // Query parameters are parsed from the original URI in the handler.
}

rpc V3SdmxData(SdmxDataRequest) returns (google.api.HttpBody) {
  option (google.api.http) = {
    get: "/sdmx/v3/data/{context}/{agency_id}/{resource_id}/{version}/{key}"
  };
}
```

The availability RPC should follow the same pattern, with fields for its own
path components. Mixer should parse and validate its supported query
parameters such as `mode`, `references`, `updatedAfter`, and
`reportingYearStartDay` from the original URI.

This avoids coupling the public SDMX query syntax to protobuf query mapping.
Mixer owns validation for known and unknown SDMX query parameters after parsing
the raw query string.

The SDMX REST OpenAPI definition currently lists these data query parameters:

```text
c
updatedAfter
firstNObservations
lastNObservations
dimensionAtObservation
attributes
measures
includeHistory
offset
limit
sort
asOf
reportingYearStartDay
```

It lists these availability query parameters:

```text
c
mode
references
updatedAfter
reportingYearStartDay
```

### Dynamic `c` filters

The `c` filter needs special attention. SDMX represents it as a dynamic
deep-object query, for example:

```text
?c[FREQ]=M&c[TIME_PERIOD]=ge:2020-01+le:2020-12
```

The component IDs are not known when the protobuf is compiled, and one
component may appear multiple times:

```text
?c[TIME_PERIOD]=ge:2015&c[TIME_PERIOD]=le:2020
```

Do not model this as `map<string, string>`. A protobuf map loses repeated
values, and standard transcoder query mapping does not provide an SDMX-specific
contract for bracket notation.

Envoy saves the original HTTP path before rewriting the request to the gRPC
method path. Read the `x-envoy-original-path` incoming gRPC metadata value and
parse its raw query string in Mixer. Preserve repeated values and treat `+` as
a literal SDMX operator character rather than applying HTML form decoding.

The same behavior must be verified through the deployed ESPv2 image. If ESPv2
does not forward the original path metadata, use the native HTTP listener
design described in [Mixer HTTP Routing and Envoy](http_routing.md).

## Temporary Query Probe

This repository now contains a feature-gated probe RPC:

```text
GET /sdmx/v3/debug/data/{context}/{agencyID}/{resourceID}/{version}/{key}
```

The probe intentionally declares path fields only. Its handler reads
`x-envoy-original-path`, parses every query parameter, groups dynamic `c[...]`
constraints, and returns the parsed result as JSON bytes in
`google.api.HttpBody`. Sensitive query values such as `key` and `api_key` are
redacted in the response.

For local Envoy, regenerate the descriptor and call the Envoy listener:

```bash
./scripts/compile_protos.sh

curl --globoff \
  -H 'Accept: application/vnd.sdmx.data+json;version=2.0.0' \
  'http://localhost:8081/sdmx/v3/debug/data/dataflow/AGENCY/FLOW/1.0.0/*?c[FREQ]=M&c[TIME_PERIOD]=ge:2015&c[TIME_PERIOD]=le:2020&dimensionAtObservation=TIME_PERIOD'
```

The expected JSON includes:

```json
{
  "queryParams": {
    "c[FREQ]": ["M"],
    "c[TIME_PERIOD]": ["ge:2015", "le:2020"],
    "dimensionAtObservation": ["TIME_PERIOD"]
  },
  "constraints": {
    "FREQ": ["M"],
    "TIME_PERIOD": ["ge:2015", "le:2020"]
  }
}
```

Also test encoded brackets and plus signs:

```bash
curl --globoff \
  'http://localhost:8081/sdmx/v3/debug/data/dataflow/AGENCY/FLOW/1.0.0/*?c%5BTIME_PERIOD%5D=ge%3A2020-01%2Ble%3A2020-12'
```

The local Envoy config already has `ignore_unknown_query_parameters: true`, so
query parameters omitted from the probe protobuf can reach the handler. For a
development ESPv2 deployment, enable the equivalent startup option:

```text
--transcoding_ignore_unknown_query_parameters=true
```

Do not enable that option globally in production without auditing existing
API behavior. The public SDMX handler should perform its own strict query
validation after parsing.

## Headers And Content Negotiation

Headers should be separated into three categories.

### Mixer-owned request headers

Mixer needs the HTTP `Accept` header to select the response representation. It
also needs `Accept-Language` and `If-Modified-Since` if the initial API claims
support for those SDMX semantics. The transcoded request should expose them as
incoming gRPC metadata. Mixer already uses incoming gRPC metadata elsewhere, so
the handler can read lower-case metadata keys such as `accept`,
`accept-language`, and `if-modified-since`.

Verify this behavior with an integration test through both local Envoy and a
development ESPv2 deployment before relying on it as a public contract.

Support both forms requested by clients:

```text
Accept: application/vnd.sdmx.data+json;version=2.0.0
Accept: application/vnd.sdmx.data+csv;version=2.0.0
```

and:

```text
?format=json
?format=csv
```

Use this precedence:

```text
format query parameter
  -> Accept header
  -> default SDMX-JSON representation
```

The `format` alias is a normal request protobuf field. The `Accept` header is
metadata, not a protobuf field.

### Gateway-owned request headers

ESPv2 should continue to handle API key authentication and other gateway
concerns before invoking Mixer. The `HttpBody` response type does not bypass
ESPv2.

The SDMX OpenAPI definition also lists `Accept-Encoding`. Compression should
remain a proxy concern unless the application needs to choose or reject a
specific encoding. Validate the supported encodings in Envoy and ESPv2 rather
than advertising the full SDMX list automatically.

### Response headers and status codes

`HttpBody` directly controls the response body and `Content-Type`. If the SDMX
contract requires additional response headers, caching behavior, or exact HTTP
status codes beyond the existing gRPC status mapping, verify how Envoy and
ESPv2 forward gRPC metadata before committing to those semantics.

## Availability Responses

The availability API can construct an SDMX-JSON document directly and return
its bytes in `HttpBody.data`.

There is no need to define the SDMX-JSON response shape as protobuf messages:

```go
payload, err := json.Marshal(sdmxAvailabilityDocument)
if err != nil {
    return nil, err
}
return &httpbody.HttpBody{
    ContentType: availabilityJSONContentType,
    Data:        payload,
}, nil
```

The Go representation used to build the document can be structs, maps, or a
formatter abstraction. That is an internal implementation decision. It is not
part of the gRPC wire contract.

## Unary Versus Streaming

Envoy supports both unary `HttpBody` responses and server streams of
`HttpBody`. For a stream, Envoy concatenates the `data` bytes from each message
and uses the `content_type` from the first message.

### Unary `HttpBody`

```proto
rpc V3SdmxData(SdmxDataRequest) returns (google.api.HttpBody);
```

Use unary responses when the complete payload can reasonably be held in
memory. This is the simplest path and matches the current formatter behavior,
which builds a complete string before returning.

Advantages:

- Small implementation change.
- Straightforward error handling before a response is sent.
- Good fit for availability responses and bounded data responses.

Tradeoff:

- Mixer holds the full serialized JSON or CSV payload in memory.

### Streaming `HttpBody`

```proto
rpc V3SdmxData(SdmxDataRequest) returns (stream google.api.HttpBody);
```

Streaming is useful when data CSV responses can be large enough that buffering
the full payload is undesirable.

Advantages:

- Mixer can emit CSV rows or JSON chunks incrementally.
- Lower peak Mixer memory usage for large results.

Tradeoffs:

- Errors after the first chunk are harder to represent cleanly to an HTTP
  client.
- JSON streaming must still produce one valid SDMX-JSON document.
- The first message must establish the content type.
- Proxy buffering and deployed ESPv2 behavior still need end-to-end
  validation.

## Recommendation

If the request compatibility spike passes, start with:

```text
availability API -> unary HttpBody with SDMX-JSON bytes
data API         -> unary HttpBody with SDMX-JSON or SDMX-CSV bytes
```

Add response-size instrumentation and define a maximum supported unary payload.
Move the data API to streaming `HttpBody` if realistic CSV responses exceed
that limit. Availability responses should remain unary unless measurements
show otherwise.

This keeps the initial design small while leaving a direct path to streaming
for large data exports.

## Repository Changes Needed For Implementation

The implementation phase should include:

1. Spike original URI forwarding and raw SDMX query parsing through local
   Envoy and development ESPv2, especially repeated `c[FREQ]`, literal `+`,
   `updatedAfter`, and `dimensionAtObservation`.
2. Vendor `google/api/httpbody.proto` under `proto/google/api/`, matching the
   existing vendored Google API protos used by `scripts/compile_protos.sh`.
3. Replace the current SDMX wrapper response with `google.api.HttpBody`.
4. Replace the wildcard route with explicit `/sdmx/v3/data/...` path fields.
5. Add the availability request and RPC.
6. Add content negotiation for `format` and `Accept`.
7. Return exact SDMX media types with raw JSON or CSV bytes.
8. Add unary response-size instrumentation and a documented limit.
9. Test direct gRPC behavior, local Envoy transcoding, and development ESPv2
   behavior. Confirm that ESPv2 forwards `x-envoy-original-path`.

No second Mixer port, Envoy route, Kubernetes Service port, or ESPv2 backend is
needed for this design if the request compatibility spike passes.

## Open Decisions

Before implementation, pin:

1. The supported SDMX REST specification version and exact media types.
2. Whether the deployed ESPv2 image forwards `x-envoy-original-path` and can
   use `--transcoding_ignore_unknown_query_parameters=true` for the SDMX
   routes without affecting unrelated APIs.
3. The supported subset of data and availability query parameters.
4. The required behavior for unsupported `Accept` values and query parameters.
5. The unary response-size limit and streaming threshold.
6. Any additional SDMX response headers or exact status-code requirements.

## References

- [Envoy gRPC-JSON transcoder: sending arbitrary content](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/grpc_json_transcoder_filter#sending-arbitrary-content)
- [Envoy gRPC-JSON transcoder API reference](https://www.envoyproxy.io/docs/envoy/latest/api-v3/extensions/filters/http/grpc_json_transcoder/v3/transcoder.proto)
- [Envoy transcoder source: preserve original path before rewriting](https://github.com/envoyproxy/envoy/blob/main/source/extensions/filters/http/grpc_json_transcoder/json_transcoder_filter.cc)
- [ESPv2 startup options](https://docs.cloud.google.com/endpoints/docs/grpc/specify-esp-v2-startup-options)
- [`google.api.HttpBody` protobuf definition](https://github.com/googleapis/googleapis/blob/master/google/api/httpbody.proto)
- [SDMX REST API repository](https://github.com/sdmx-twg/sdmx-rest)
- [SDMX REST OpenAPI definition](https://github.com/sdmx-twg/sdmx-rest/blob/master/api/sdmx-rest.yaml)
