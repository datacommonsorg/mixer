# Deploying Embeddings Sidecar for Mixer

## Overview

Based on the routing configuration in `values.yaml`, the `/v2/resolve` endpoint is served exclusively by the `recon` service group.

**Decision:** The sidecar container **only needs to be added to the `dc-mixer-recon` workload**. Adding it to other workloads (like `observation` or `node`) is unnecessary as they do not serve the resolution endpoint.

## Required Modifications

### 1. Update `deploy/helm_charts/mixer/values.yaml`

Add the `embeddingsSidecar` configuration to the root level (or under `mixer` if preferred, but adjust access paths accordingly).

```yaml
embeddingsSidecar:
  enabled: false
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
  enabled: true
  env:
    vertex_ai_models:
      uae-large-v1-model:
        prediction_endpoint_id: "8435477397553283072"
```

### Autopush Configuration (`deploy/helm_charts/envs/mixer_autopush.yaml`)

```yaml
embeddingsSidecar:
  enabled: true
  env:
    vertex_ai_models:
      uae-large-v1-model:
        prediction_endpoint_id: "8110162693219942400"
```

### Dev Configuration (`deploy/helm_charts/envs/mixer_dev.yaml`)

```yaml
embeddingsSidecar:
  enabled: true
  env:
    vertex_ai_models:
      uae-large-v1-model:
        prediction_endpoint_id: "1400502935879680000"
```

### Production Configuration (`deploy/helm_charts/envs/mixer_prod.yaml`)

```yaml
embeddingsSidecar:
  enabled: true
  env:
    vertex_ai_models:
      uae-large-v1-model:
        prediction_endpoint_id: "430892009855647744"
```

### 2. Update `deploy/helm_charts/mixer/templates/deployment.yaml`

You need to add the sidecar container definition **and** the volume definition if specific configs are needed.

**A. Add the container to `spec.template.spec.containers`:**

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
             optional: true # Set to true if it might not exist
```

### 3. Create/Update ConfigMap in `deploy/helm_charts/mixer/templates/config_maps.yaml`

You need to pass the `env` and `catalog` values to the sidecar, likely via a ConfigMap that is mounted as a file.

```yaml
{{- if .Values.embeddingsSidecar.enabled }}
apiVersion: v1
kind: ConfigMap
metadata:
  name: mixer-embeddings-config
  namespace: {{ .Values.namespace.name }}
data:
  # env.yaml contains environment-specific settings (like Vertex AI Endpoint IDs) defined in your env files.
  env.yaml: |
    {{- toYaml .Values.embeddingsSidecar.env | nindent 4 }}
{{- end }}
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
*   `mixer-robot@datcom-mixer-prod.iam.gserviceaccount.com`
*   `mixer-robot@datcom-mixer-dev.iam.gserviceaccount.com`

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

## Remainig Todos
* Grant mixer service account the permissions in datcom-nl
* push to dev and test