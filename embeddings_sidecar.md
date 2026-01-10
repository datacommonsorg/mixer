# Deploying Embeddings Sidecar for Mixer

## Overview

Based on the routing configuration in `values.yaml`, the `/v2/resolve` endpoint is served exclusively by the `recon` service group.

**Decision:** The sidecar container **only needs to be added to the `dc-mixer-recon` workload**. Adding it to other workloads (like `observation` or `node`) is unnecessary as they do not serve the resolution endpoint.

## Required Modifications

### 0. Feature Flags

Ideally you should also enable the `EnableEmbeddingsResolver` feature flag in `deploy/featureflags/` for the environment you are deploying to (e.g. `dev.yaml`, `autopush.yaml`).

```yaml
flags:
  # ...
  EnableEmbeddingsResolver: true
```

### 1. Update `deploy/helm_charts/mixer/values.yaml`

Add the `embeddingsSidecar` configuration to the root level (or under `mixer` if preferred, but adjust access paths accordingly).

```yaml
embeddingsSidecar:
  image:
    repository: gcr.io/datcom-ci/datacommons-nl
    pullPolicy: IfNotPresent
    tag: "mixer-sidecar"
  memory: "4G"
  workers: 1
  env:
    default_indexes:
      - base_uae_mem
    enabled_indexes:
      - base_uae_mem
      - medium_ft
      - sdg_ft
      - undata_ft
    vertex_ai_models:
      uae-large-v1-model:
        project_id: datcom-nl
        location: us-central1
        prediction_endpoint_id: "" # Must be defined per environment
    enable_reranking: false
```

**Update `serviceGroups.recon` to enable it:**

### Staging Configuration (`deploy/helm_charts/envs/mixer_staging.yaml`)

```yaml
embeddingsSidecar:
  env:
    vertex_ai_models:
      uae-large-v1-model:
        prediction_endpoint_id: "8435477397553283072"
```

### Autopush Configuration (`deploy/helm_charts/envs/mixer_autopush.yaml`)

```yaml
embeddingsSidecar:
  env:
    vertex_ai_models:
      uae-large-v1-model:
        prediction_endpoint_id: "8110162693219942400"
```

### Dev Configuration (`deploy/helm_charts/envs/mixer_dev.yaml`)

```yaml
embeddingsSidecar:
  env:
    vertex_ai_models:
      uae-large-v1-model:
        prediction_endpoint_id: "1400502935879680000"
```

### Production Configuration (`deploy/helm_charts/envs/mixer_prod.yaml`)

```yaml
embeddingsSidecar:
  env:
    vertex_ai_models:
      uae-large-v1-model:
        prediction_endpoint_id: "430892009855647744"
```

### 2. Update `deploy/helm_charts/mixer/templates/deployment.yaml`

You need to add the sidecar container definition **and** the volume definition if specific configs are needed.

**A. Add the container to `spec.template.spec.containers`:**

The container is conditionally added specifically for the `recon` workload using `$group.enableEmbeddingsSidecar`.

```yaml
      containers:
        # ... mixer and esp containers ...

        {{- if $group.enableEmbeddingsSidecar }}
        - name: embeddings
          image: "{{ $.Values.embeddingsSidecar.image.repository }}{{- if $.Values.embeddingsSidecar.image.tag }}:{{ $.Values.embeddingsSidecar.image.tag }}{{- end }}"
          imagePullPolicy: {{ $.Values.embeddingsSidecar.image.pullPolicy }}
          resources:
            limits:
              memory: {{ $.Values.embeddingsSidecar.memory }}
            requests:
              memory: {{ $.Values.embeddingsSidecar.memory }}
          env:
            - name: NUM_WORKERS
              value: {{ $.Values.embeddingsSidecar.workers | quote }}
            - name: ADDITIONAL_CATALOG_PATH
              # Required to instruct the app to load the catalog that's baked into the image.
              value: "/workspace/catalog.yaml"
          ports:
            - containerPort: 6060
          startupProbe:
            httpGet:
              path: /healthz
              port: 6060
            failureThreshold: 30
            periodSeconds: 10
            timeoutSeconds: 5
          readinessProbe:
            httpGet:
              path: /healthz
              port: 6060
            timeoutSeconds: 300
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /healthz
              port: 6060
            timeoutSeconds: 300
            periodSeconds: 10
          volumeMounts:
            - name: embeddings-config
              mountPath: /datacommons/nl
        {{- end }}
```

**B. Add the Volume to `spec.template.spec.volumes`:**

You must define the `embeddings-config` volume. If this points to a ConfigMap, ensure that ConfigMap exists or is created in `config_maps.yaml`.

```yaml
      volumes:
        # ... existing volumes ...
        - name: embeddings-config
          configMap:
             name: mixer-embeddings-config # Make sure this ConfigMap exists!
```

### 3. Create/Update ConfigMap in `deploy/helm_charts/mixer/templates/config_maps.yaml`

You need to pass the `env` and `catalog` values to the sidecar, likely via a ConfigMap that is mounted as a file.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mixer-embeddings-config
  namespace: {{ .Release.Namespace }}
data:
  # env.yaml contains environment-specific settings (like Vertex AI Endpoint IDs) defined in your env files.
  env.yaml: |
    {{- toYaml .Values.embeddingsSidecar.env | nindent 4 }}
```

## Configuration Notes

*   **Port:** Confirmed **6060** (via Dockerfile `CMD exec gunicorn ... --bind :6060`).
*   **NUM_WORKERS:** This environment variable is **REQUIRED** as it is used in the Dockerfile `CMD` for worker validation (`$((NUM_WORKERS + 0))`).
*   **ConfigMap:** The `env` and `catalog` configurations are mounted as files. Ensure your application logic (`nl_app.py` or `nl_server`) is configured to read these files from `/datacommons/nl/`.

## High-Level Concept: What is `env.yaml`?

`env.yaml` is **NOT** a native Kubernetes or GKE concept. It is a **custom configuration file** specific to the `nl_server` Python application.

1.  **GKE's Role:** GKE simply takes the data from your ConfigMap and creates a file named `env.yaml` at `/datacommons/nl/env.yaml`. GKE doesn't know or care what's inside it.
2.  **App's Role:** The `nl_server` Python code has logic that explicitly looks for a file at that path. When found, it parses the YAML to load variables like `vertex_ai_models` and their endpoint IDs.

This file acts as the "bridge" between your Helm values (where you define infrastructure diffs) and the application code (which needs those values to function).

## Catalog Loading Logic
 
 We exclusively use the `catalog.yaml` baked into the image.
 
 *   **Configuration:** We set `ADDITIONAL_CATALOG_PATH: /workspace/catalog.yaml` in the deployment environment variables.
 *   **Discovery:** The application (`nl_server`) finds this path via the environment variable and loads the built-in catalog.
 *   **No Override:** We do NOT mount an external catalog via ConfigMap.

## IAM Requirements

The sidecar uses the pod's Workload Identity (running as `mixer-robot`) to authenticate with Vertex AI.
You must grant the **Vertex AI User** (`roles/aiplatform.user`) role to the `mixer-robot` service account of *each* environment in the **Model Host Project** (`datcom-nl`).

**Principals to Authorize in `datcom-nl`:**
*   `mixer-robot@datcom-mixer-autopush.iam.gserviceaccount.com`
*   `mixer-robot@datcom-mixer-staging.iam.gserviceaccount.com`
*   `mixer-robot@datcom-mixer.iam.gserviceaccount.com`
*   `mixer-robot@datcom-mixer-dev-316822.iam.gserviceaccount.com`

https://pantheon.corp.google.com/iam-admin/iam?project=datcom-nl 

**Role:** `Vertex AI User`
*   This grants permission to call `aiplatform.endpoints.predict`, which is required to fetch embeddings.
*   "Discovery Viewers" is generally *not* required for standard Endpoint prediction unless you are using specific Discovery Engine features.

## Verification

After applying these changes, you can verify the deployment:

1.  **Deploy changes:** Run your standard deployment script (e.g., `autopush` or `dev`).
2.  **Check Pods:** Ensure `dc-mixer-recon` pods have 3 containers (mixer, esp, embeddings).
    ```bash
    kubectl get pods -l service=dc-mixer-recon -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{range .spec.containers}{"- "}{.name}{"\n"}{end}{"\n"}{end}'
    ```
3.  **Test Connectivity:** From within the `mixer` container in the `recon` pod, try to hit the sidecar:
    ```bash
    kubectl exec -it <recon-pod-name> -c mixer -- curl localhost:6060/healthz
    ```

## Resolve Endpoint Expansion: Design & Implementation Plan

### 1. Objective
Expand the `/v2/resolve` endpoint to support "smart" resolution using the Embeddings Sidecar. This allows resolving natural language queries (e.g., "population of california") to Statistical Variables (SVs) via vector search, separate from the existing ID/Coordinate/Description resolution logic.

### 2. API Schema Changes (`proto/v2/resolve.proto`)

We need to extend the Protocol Buffers definition to support the new request parameter and response metadata.

**A. Update `ResolveRequest`**
Add an optional `resolver` field to explicitly select the resolution strategy (though we will also support implicit routing).

```protobuf
message ResolveRequest {
    repeated string nodes = 1;
    string property = 2;
    string resolver = 3; // New field: "place" or "indicator" (default)
}
```

**B. Update `ResolveResponse`**
Add a `metadata` field to the `Candidate` message to return confidence scores and matching sentences, which are critical for debugging and UI highlighting.

```protobuf
message ResolveResponse {
    message Entity {
        message Candidate {
            string dcid = 1;
            string dominant_type = 2;
            // New field: Arbitrary metadata for debugging/UI (e.g., scores, sentence matches)
            // Using Map<string, string> for flexibility.
            map<string, string> metadata = 3;
        }
        string node = 1;
        repeated Candidate candidates = 3;
        // ...
    }
    repeated Entity entities = 1;
}
```

### 3. Routing Logic (`internal/server/handler_core.go`)

We will modify `V2ResolveCore` to implement the following decision tree:

1.  **Check explicit `resolver` param:**
    *   If `resolver == "place"`: Force Legacy Logic.
    *   If `resolver == "indicator"`: Force New Embeddings Logic.
2.  **Check `property` param (Implicit Fallback):**
    *   If `property` is **NOT EMPTY**: Assume Legacy Logic (existing behavior for ID/Coordinate/Description resolution).
    *   If `property` is **EMPTY** (and `resolver` is not "place"): Default to New Embeddings Logic.

**Pseudocode:**
```go
func (s *Server) V2ResolveCore(ctx, in) {
    if in.GetResolver() == "place" || in.GetProperty() != "" {
        // ... Existing Legacy Logic (ParseProperty, Switch on SingleProp) ...
    } else {
        // ... New Logic ...
        return resolve.ResolveEmbeddings(ctx, s.httpClient, in.GetNodes())
    }
}
```

### 4. Embeddings Logic (`internal/server/v2/resolve/embeddings.go`)

Create a new file `internal/server/v2/resolve/embeddings.go` to handle the interaction with the sidecar.

**A. Sidecar Client Structures**
Define Go structs to match the Sidecar's JSON API.
```go
type EmbeddingsRequest struct {
    Queries []string `json:"queries"`
}

type EmbeddingsResponse struct {
    QueryResults map[string]struct {
        SV            []string `json:"SV"`
        CosineScore   []float64 `json:"CosineScore"`
        SVToSentences map[string][]struct {
            Sentence string  `json:"sentence"`
            Score    float64 `json:"score"`
        } `json:"SV_to_Sentences"`
    } `json:"queryResults"`
}
```

**B. `ResolveEmbeddings` Function**
1.  **Construct Request:** Create `EmbeddingsRequest` from input `nodes`.
2.  **Call Sidecar:** `--embeddings_server_url="http://localhost:6060"/` (use `s.httpClient`).
3.  **Process Response:** Iterate through `EmbeddingsResponse.QueryResults` and map to `pbv2.ResolveResponse`.
    *   **Candidate Mapping:**
        *   `dcid` <- `SV[i]`
        *   `dominant_type` <- "StatisticalVariable" (hardcoded for now as this is specifically for SV resolution).
        *   `metadata`: Populate with "score" (from `CosineScore[i]`) and "sentence" (from `SVToSentences`).

### 5. Verification Plan

**A. Generate Protos**
Run the repository's proto generation script (usually `go generate ./...` or `make gen`) to update `.pb.go` files.

**B. Unit Tests**
*   Create `internal/server/v2/resolve/embeddings_test.go`.
*   Use `httptest` to mock the Sidecar's HTTP response.
*   Verify that `ResolveEmbeddings` correctly parses the JSON and populates the Proto response.

**C. Manual Integration Test**
1.  Deploy changes to `mixer-dev` or run locally with sidecar enabled.
2.  **Legacy Check:** `curl .../v2/resolve?property=<-geoCoordinate->dcid ...` -> Should work as before.
3.  **New Logic Check:** `curl .../v2/resolve` (with body `{"nodes": ["population"]}`) -> Should hit sidecar path.
4.  **Explicit Resolver Check:** `curl .../v2/resolve?resolver=indicator` -> Should hit sidecar path.

### 6. Implementation Steps
1.  **Proto:** Modify `resolve.proto` and regenerate.
2.  **Logic:** Create `embeddings.go` with client logic.
3.  **Routing:** Update `handler_core.go` to wire it up.
4.  **Deploy:** Build and push new Mixer image.

## Usage Guide: Smart Resolve Endpoint

The `/v2/resolve` endpoint has been expanded to support **Smart Resolution** (Indicator/Statistical Variable resolution) using the embeddings sidecar. This allows users to resolve natural language queries to Statistical Variables.

### Endpoint
`POST /v2/resolve` or `GET /v2/resolve`

### Request Parameters

| Parameter | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `nodes` | List[String] | Yes | The strings to resolve (e.g., "population", "median income"). |
| `resolver` | String | No | The resolution strategy to use.<br>• `indicator`: **Forces** Smart Resolution (Embeddings).<br>• `place`: **Forces** Legacy Resolution (ID/Coordinate/Description).<br>• `[Empty]`: Defaults to Smart Resolution if `property` is also empty. |
| `property` | String | No | **Legacy Parameter.** If present, triggers Legacy Resolution. Examples: `<-geoCoordinate->dcid`, `<-description->dcid`. |

### Response Format (Smart Resolution)

The response follows the standard `ResolveResponse` structure but utilizes the `metadata` field for scoring and sentence matching.

```json
{
  "entities": [
    {
      "node": "user query string",
      "candidates": [
        {
          "dcid": "StatisticalVariable_DCID",
          "dominantType": "StatisticalVariable",
          "metadata": {
            "score": "0.89",           // Score of the highest scoring sentence (0.0 - 1.0)
            "sentence": "matched text"  // The actual text description that matched
          }
        }
      ]
    }
  ]
}
```

### Examples

#### 1. Smart Resolution (Implicit)
Resolving a metric name without specifying a property.
**Request:**
```bash
curl "http://localhost:8081/v2/resolve?nodes=population"
```
**Response:**
```json
{
  "entities": [
    {
      "node": "population",
      "candidates": [
        {
          "dcid": "Count_Person",
          "dominantType": "StatisticalVariable",
          "metadata": { "score": "0.98", "sentence": "population count" }
        }
      ]
    }
  ]
}
```

#### 2. Smart Resolution (Explicit)
Explicitly requesting the indicator resolver. Useful if you want to be certain of the behavior.
**Request:**
```bash
curl "http://localhost:8081/v2/resolve?nodes=median%20income&resolver=indicator"
```

#### 3. Legacy Resolution (Backwards Compatibility)
Coordinate resolution (Legacy) works exactly as before.
**Request:**
```bash
curl "http://localhost:8081/v2/resolve?nodes=37.42,-122.08&property=<-geoCoordinate->dcid"
```


## Remaining Todos
* push to dev and test it out
* revisit the new resolve endpoint contract
* create automatic tagging for the embeddings server