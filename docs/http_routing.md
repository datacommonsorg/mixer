# Mixer HTTP Routing and Envoy

This document describes the current Mixer request path and the recommended way
to add native HTTP endpoints. "Native HTTP" means that Mixer handles the HTTP
request directly instead of receiving a gRPC request produced by JSON
transcoding. The GKE diagrams start at Mixer's ingress. The optional Apigee
facade configured under [`deploy/apigee`](../deploy/apigee) can sit farther
upstream of that ingress.

## Summary

Mixer currently exposes only a gRPC application server on port `12345`.

| Environment | Current REST request path | Is the proxy required? |
| --- | --- | --- |
| Local development | Client -> Envoy `:8081` -> Mixer gRPC `:12345` | Only when testing the REST surface. A gRPC client can call Mixer directly. |
| GKE | Ingress -> Kubernetes Service -> ESPv2 `:8081` -> Mixer gRPC `:12345` | Yes for external REST traffic. ESPv2 performs API management as well as transcoding. |

For a future native HTTP namespace such as `/sdmx`, Mixer can listen on a
separate port such as `15001`. Trusted local tests can call that port directly.
External GKE traffic should still pass through ESPv2 so that API key checks,
quotas, telemetry, and other gateway behavior remain in place.

## Current Local Request Path

The local development proxy is configured in [`esp/envoy-config.yaml`](../esp/envoy-config.yaml).
It listens on `0.0.0.0:8081`, applies Envoy's
[`grpc_json_transcoder`](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/grpc_json_transcoder_filter)
filter, and forwards requests to the `grpcserver` cluster on port `12345`.
That cluster enables HTTP/2 because gRPC uses HTTP/2.

```text
REST client
  |
  | HTTP/JSON :8081
  v
local Envoy
  |
  | gRPC over HTTP/2 :12345
  v
Mixer
```

The Mixer process itself starts its gRPC listener in [`cmd/main.go`](../cmd/main.go).
The local Envoy process is started separately, as documented in
[`docs/developer_guide.md`](developer_guide.md).

Local Envoy is useful but optional:

- Use `localhost:12345` directly for a gRPC client.
- Use `localhost:8081` through Envoy when testing the existing REST API.
- For a future native HTTP server on `localhost:15001`, call that port directly
  when testing only the HTTP handler.
- Route the native HTTP request through local Envoy when testing the complete
  local public path from one listener.

## Current GKE Request Path

The GKE path does not use [`esp/envoy-config.yaml`](../esp/envoy-config.yaml).
Instead, [`scripts/deploy_gke.sh`](../scripts/deploy_gke.sh) deploys the Mixer
protobuf descriptor and [`esp/endpoints.yaml.tmpl`](../esp/endpoints.yaml.tmpl)
to Cloud Endpoints. It downloads the resulting service configuration into the
`service-config-configmap` ConfigMap.

The Helm chart mounts that configuration into an ESPv2 sidecar. Each Mixer pod
contains:

- The Mixer container, with a gRPC listener on `12345`.
- The ESPv2 container, with an HTTP listener on `8081`.
- ESPv2's `--backend=grpc://127.0.0.1:12345` argument.

The chart creates one deployment and Kubernetes Service per Mixer service
group. GKE Ingress selects a service group by URL path. Each Service sends
traffic to the pod's ESPv2 port `8081`, not directly to Mixer.

```text
external REST client
  |
  v
GKE Ingress
  |
  | select service group by URL path
  v
Kubernetes Service
  |
  | HTTP/JSON :8081
  v
ESPv2 sidecar
  |
  | API management and JSON transcoding
  | gRPC over HTTP/2 :12345
  v
Mixer
```

ESPv2 is based on Envoy, but it has a broader role than the local Envoy config.
It applies the deployed Cloud Endpoints service configuration. That is where
API management behavior such as API key validation, quotas, and gateway
telemetry belongs. See the
[ESPv2 repository](https://github.com/GoogleCloudPlatform/esp-v2) and the
[Cloud Endpoints documentation](https://cloud.google.com/endpoints/docs/openapi/about-cloud-endpoints).

## Adding Native HTTP Endpoints

Assume Mixer adds an HTTP server on `15001` and owns requests under `/sdmx`.
The full incoming path should be preserved.

### Local Development

For normal local development of a native HTTP endpoint, call Mixer directly:

```text
HTTP client -> Mixer native HTTP :15001
```

No Envoy change is required. This is the preferred path while implementing and
testing the HTTP handler.

Optionally extend [`esp/envoy-config.yaml`](../esp/envoy-config.yaml) when an
end-to-end local test should use one public-style entry point for both native
HTTP and transcoded gRPC APIs. Add the native HTTP route before the existing `/`
catch-all route:

```yaml
- match:
    prefix: "/sdmx"
  typed_per_filter_config:
    envoy.filters.http.grpc_json_transcoder:
      "@type": type.googleapis.com/envoy.config.route.v3.FilterConfig
      disabled: true
  route:
    cluster: httpserver
    timeout: 60s
```

Add a plain HTTP/1.1 cluster:

```yaml
- name: httpserver
  connect_timeout: 60s
  type: logical_dns
  lb_policy: round_robin
  dns_lookup_family: V4_ONLY
  load_assignment:
    cluster_name: httpserver
    endpoints:
      - lb_endpoints:
          - endpoint:
              address:
                socket_address:
                  address: 127.0.0.1
                  port_value: 15001
```

Do not add `http2_protocol_options` to this cluster. The upstream connection
should use ordinary HTTP/1.1.

Envoy passes through requests that do not match a configured gRPC method by
default. Disabling the transcoder explicitly for `/sdmx` is still preferable:
it documents the intended behavior and prevents a later protobuf binding from
silently changing the route.

For a strict URL namespace, prefer separate exact `/sdmx` and prefix `/sdmx/`
matches. A single `prefix: "/sdmx"` match also includes paths such as
`/sdmx-test`.

### GKE

Do not route external `/sdmx` requests directly from GKE Ingress to Mixer
`:15001`. That would technically serve HTTP, but it would bypass ESPv2's API
key checks, quotas, and telemetry.

The recommended production request path is:

```text
/sdmx external client
  |
  v
GKE Ingress
  |
  v
ESPv2 HTTP entry point
  |
  | API management, without gRPC transcoding
  v
Mixer native HTTP :15001
```

Keep the existing ESPv2-to-gRPC path unchanged for the existing API surface.
Expose `/sdmx` through an ESPv2 HTTP API configuration, normally described by
an OpenAPI document, and forward that managed route to Mixer `:15001`.

The current sidecar starts with one local backend:
`--backend=grpc://127.0.0.1:12345`. ESPv2 documents this single-local-backend
sidecar model in its
[repository](https://github.com/GoogleCloudPlatform/esp-v2#features).
The conservative extension is therefore another ESPv2 container/listener or a
dedicated ESPv2 deployment for `/sdmx`, selected by a dedicated Kubernetes
Service and Ingress path. That Service must target the HTTP ESPv2 listener, not
Mixer `:15001`. Do not assume the local Envoy YAML is used by GKE.

If a single ESPv2 instance routing to both local ports is desirable, validate
that as a separate ESPv2 configuration spike before adopting it. It is more
coupled to generated ESPv2 configuration and is not how the current Helm chart
is wired.

## Do Native HTTP Calls Need Envoy?

Not inherently. A native HTTP server can be called directly.

The correct choice depends on the trust boundary:

| Caller | Direct call to Mixer `:15001`? | Recommendation |
| --- | --- | --- |
| Local handler test | Yes | Call Mixer directly for a fast test. |
| Local end-to-end test | Usually no | Route through local Envoy to exercise path routing. |
| Trusted in-cluster caller | Possibly | Allow only when intentionally bypassing public API management. |
| External API caller | No | Keep ESPv2 in the path. |

Plain Envoy routing is enough when the requirement is only forwarding. ESPv2
is needed when the route must retain Cloud Endpoints API management.

## Consider Staying With gRPC Transcoding

Before adding a second Mixer listener, check whether native HTTP is necessary.
If the only requirement is returning a custom payload or content type, keeping
the existing gRPC transport is operationally simpler. A transcoded gRPC method
can return [`google.api.HttpBody`](https://github.com/googleapis/googleapis/blob/master/google/api/httpbody.proto)
for an arbitrary HTTP response body. This repo vendors
`proto/google/api/httpbody.proto` beside the existing Google API protos so
`scripts/compile_protos.sh` can resolve that import reproducibly.

See [SDMX HTTP Responses Over gRPC](sdmx_httpbody_design.md) for the detailed
design, including GET query parameters, headers, and unary versus streaming
responses.

Use a native HTTP listener when the endpoint genuinely needs HTTP behavior that
does not fit the gRPC gateway model, such as a protocol-defined REST surface or
HTTP-specific response semantics.

## Recommended Direction for `/sdmx`

1. Keep the existing gRPC listener on `12345`.
2. Validate the SDMX query probe through local Envoy and development ESPv2.
3. If the probe passes, use transcoded gRPC with `google.api.HttpBody`; do not
   add a second Mixer port.
4. Add a native Mixer HTTP listener on `15001` only if the deployed ESPv2 path
   cannot preserve the required SDMX request semantics.
5. For the native-listener fallback, call Mixer `:15001` directly during normal
   local development and add an Envoy `/sdmx` pass-through route for
   end-to-end testing.
6. In GKE, preserve ESPv2 in front of `/sdmx`; do not bypass it.
