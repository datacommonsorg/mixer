# Design Doc: Configurable Mixer Settings (`mixer_config.yaml`)

## Objective
To add support for a service-specific configuration file (`mixer_config.yaml`) in Mixer. This enables dynamic, Git-tracked customization of:
1.  **Embeddings index mapping** (resolving request headers to index names).
2.  **Spanner vector search parameters** (thresholds, limits, model names).

It also cleans up code-embedded configuration anti-patterns (such as `runtime.Caller` relative file-path lookups) and decouples Mixer application settings from Terraform variables.

---

## Detailed Design

### 1. The Configuration File Structure (`mixer_config.yaml`)
The configuration file will contain static, non-sensitive settings, grouped under a unified `vector_search` namespace, but strictly separating the legacy embeddings server and Spanner backends. The Spanner settings are flattened (removing the nested `vector_search_config` block):

```yaml
# mixer_config.yaml

vector_search:
  embeddings_server:
    server_url: "http://localhost:5001"
    default_index: "base_multi_entity"
    resolve_index_mapping:
      multi-entity: "base_multi_entity"
      base-nl: "base_uae_mem"
  
  spanner_settings:
    default_index: "multi-entity"
    indexes:
      multi-entity:
        search_algorithm: "vector_search"
        embedding_table: "NodeEmbedding"
        embedding_model: "NodeEmbeddingModel"
        embedding_type: "RETRIEVAL_QUERY"
        vector_search_algo: "ANN"
        limit: 50
        num_leaves: 10
        threshold: 0.5
        postprocessing:
          - "none"
```

---

### 2. Go Structs & Schema Definition
We will define the configuration schema in a new package `internal/config/config.go`. Pointers are used to distinguish between "omitted" and "zero-value":

```go
package config

type MixerConfig struct {
	VectorSearch *VectorSearchConfig `yaml:"vector_search"`
}

type VectorSearchConfig struct {
	EmbeddingsServer *EmbeddingsServerConfig `yaml:"embeddings_server"`
	SpannerSettings  *SpannerSettingsConfig  `yaml:"spanner_settings"`
}

type EmbeddingsServerConfig struct {
	ServerURL           *string           `yaml:"server_url"`
	DefaultIndex        *string           `yaml:"default_index"`
	ResolveIndexMapping map[string]string `yaml:"resolve_index_mapping"`
}

type SpannerSettingsConfig struct {
	DefaultIndex *string                           `yaml:"default_index"`
	Indexes      map[string]*SpannerIndexConfigDetail `yaml:"indexes"`
}

type SpannerIndexConfigDetail struct {
	SearchAlgorithm  *string  `yaml:"search_algorithm"`
	EmbeddingModel   *string  `yaml:"embedding_model"`
	EmbeddingTable   *string  `yaml:"embedding_table"`
	EmbeddingType    *string  `yaml:"embedding_type"`
	VectorSearchAlgo *string  `yaml:"vector_search_algo"`
	Limit            *int     `yaml:"limit"`
	NumLeaves        *int     `yaml:"num_leaves"`
	Threshold        *float64 `yaml:"threshold"`
	Postprocessing   []string `yaml:"postprocessing"`
}
```
```

---

### 3. File Loader (GCS & Local Fallback)
A new utility `ReadFile` will be implemented to load configuration bytes. It will detect the `gs://` prefix to fetch the file from GCS, otherwise falling back to standard local file reading:

```go
// internal/config/util.go

func ReadFile(ctx context.Context, path string) ([]byte, error) {
	if strings.HasPrefix(path, "gs://") {
		// Use GCS Storage client to download file...
	}
	return os.ReadFile(path)
}
```

---

### 4. Spanner Search Refactor

#### A. Baked-in Go Defaults
We will move the default Spanner search settings from `default.yaml` into Go code as a fallback, and delete the `spanner_config` directory. The internal `SpannerSearchConfig` struct will also be flattened:

```go
// internal/server/spanner/embedding_config.go

type SpannerSearchConfig struct {
	SearchAlgorithm  SearchMethod         `json:"search_algorithm" yaml:"search_algorithm"`
	EmbeddingModel   string               `json:"embedding_model" yaml:"embedding_model"`
	EmbeddingTable   string               `json:"embedding_table" yaml:"embedding_table"`
	EmbeddingType    EmbeddingType        `json:"embedding_type" yaml:"embedding_type"`
	VectorSearchAlgo VectorSearchAlgo     `json:"vector_search_algo" yaml:"vector_search_algo"`
	Limit            int                  `json:"limit" yaml:"limit"`
	NumLeaves        int                  `json:"num_leaves" yaml:"num_leaves"`
	Threshold        float64              `json:"threshold" yaml:"threshold"`
	Postprocessing   []PostprocessingType `json:"postprocessing" yaml:"postprocessing"`
}

func DefaultSpannerSearchConfig() *SpannerSearchConfig {
	return &SpannerSearchConfig{
		SearchAlgorithm:  "vector_search",
		EmbeddingModel:   "NodeEmbeddingModel",
		EmbeddingTable:   "NodeEmbedding",
		EmbeddingType:    "RETRIEVAL_QUERY",
		VectorSearchAlgo: "ANN",
		Limit:            50,
		NumLeaves:        10,
		Threshold:        0.5,
		Postprocessing:   []PostprocessingType{PostprocessingNone},
	}
}
```

#### B. Delegated Resolution
`spanner.ResolveConfig` will merge the user configuration map from YAML with defaults:

```go
// internal/server/spanner/embedding_config.go

func ResolveConfig(
    userIndexes map[string]*config.SpannerIndexConfigDetail,
) (map[string]*SpannerSearchConfig, error) {
    resolved := make(map[string]*SpannerSearchConfig)
    for label, userCfg := range userIndexes {
        cfg := DefaultSpannerSearchConfig()
        // Merge userCfg into cfg (using pointer checks)
        if userCfg.SearchAlgorithm != nil {
            cfg.SearchAlgorithm = SearchMethod(*userCfg.SearchAlgorithm)
        }
        if userCfg.EmbeddingModel != nil {
            cfg.EmbeddingModel = *userCfg.EmbeddingModel
        }
        if userCfg.EmbeddingTable != nil {
            cfg.EmbeddingTable = *userCfg.EmbeddingTable
        }
        if userCfg.EmbeddingType != nil {
            cfg.EmbeddingType = EmbeddingType(*userCfg.EmbeddingType)
        }
        if userCfg.VectorSearchAlgo != nil {
            cfg.VectorSearchAlgo = VectorSearchAlgo(*userCfg.VectorSearchAlgo)
        }
        if userCfg.Limit != nil {
            cfg.Limit = *userCfg.Limit
        }
        if userCfg.NumLeaves != nil {
            cfg.NumLeaves = *userCfg.NumLeaves
        }
        if userCfg.Threshold != nil {
            cfg.Threshold = *userCfg.Threshold
        }
        // ... merge postprocessing if needed
        resolved[label] = cfg
    }
    return resolved, nil
}
```

#### C. Spanner DataSource Integration
`NewSpannerDataSource` will accept the resolved map and the default index label:

```go
func NewSpannerDataSource(
	client SpannerClient,
	recogPlaceStore *files.RecogPlaceStore,
	mapsClient maps.MapsClient,
	searchConfigs map[string]*SpannerSearchConfig,
	defaultLabel string,
) *SpannerDataSource
```

---

### 5. Embeddings Resolver Refactor

We need to resolve the legacy embeddings configuration (server URL and index mappings) from the new unified config.

#### A. Delegated Resolution
`resolve.ResolveConfig` will merge the legacy embeddings config with CLI flags:

```go
// internal/server/v2/resolve/embeddings.go

type ResolvedEmbeddingsConfig struct {
    ServerURL           string
    DefaultIndex        string
    ResolveIndexMapping map[string]string
}

func ResolveConfig(
    userCfg *config.EmbeddingsServerConfig,
    flagDefaultIndex, flagServerURL string,
) *ResolvedEmbeddingsConfig {
    cfg := &ResolvedEmbeddingsConfig{
        ResolveIndexMapping: make(map[string]string),
    }

    if userCfg != nil {
        if userCfg.ServerURL != nil {
            cfg.ServerURL = *userCfg.ServerURL
        }
        if userCfg.DefaultIndex != nil {
            cfg.DefaultIndex = *userCfg.DefaultIndex
        }
        cfg.ResolveIndexMapping = userCfg.ResolveIndexMapping
    }

    // CLI Flags Override
    if flagDefaultIndex != "" {
        cfg.DefaultIndex = flagDefaultIndex
    }
    if flagServerURL != "" {
        cfg.ServerURL = flagServerURL
    }

    return cfg
}
```

#### B. Index Selector Integration
Update `SelectEmbeddingsIndex` in `embeddings.go` to receive the resolved config's index mapping:

```go
func SelectEmbeddingsIndex(ctx context.Context, mapping map[string]string, defaultIndex string) (string, error) {
	label := util.GetSingleHeaderValue(ctx, util.XV2ResolveIndex)
	if label == "" {
		return defaultIndex, nil
	}
	if indexName, ok := mapping[label]; ok {
		return indexName, nil
	}
	return "", status.Errorf(codes.InvalidArgument, "Invalid V2Resolve index label: %s", label)
}
```

---

### 6. Main Integration (`cmd/main.go`)

In `main()`, we load the unified config and pass resolved configs to Spanner and Mixer Server:

```go
// 1. Add CLI Flag
configPath = flag.String("config_path", "", "Path to mixer_config.yaml (local path or gs:// URI)")

// 2. Parse config file (if flag set)
var parsedConfig *config.MixerConfig
if *configPath != "" {
    bytes, _ := config.ReadFile(ctx, *configPath)
    yaml.Unmarshal(bytes, &parsedConfig)
}

// 3. Resolve Spanner Search settings
var spannerIndexes map[string]*config.SpannerIndexConfigDetail
defaultSpannerLabel := ""
if parsedConfig.GetVectorSearch() != nil && parsedConfig.GetVectorSearch().GetSpannerSettings() != nil {
    spannerIndexes = parsedConfig.GetVectorSearch().GetSpannerSettings().GetIndexes()
    if parsedConfig.GetVectorSearch().GetSpannerSettings().DefaultIndex != nil {
        defaultSpannerLabel = *parsedConfig.GetVectorSearch().GetSpannerSettings().DefaultIndex
    }
}
spannerSearchConfigs, _ := spanner.ResolveConfig(spannerIndexes)

// 4. Instantiate Spanner Data Source
spannerDS = spanner.NewSpannerDataSource(..., spannerSearchConfigs, defaultSpannerLabel)

// 5. Resolve Embeddings settings
var embeddingsServerCfg *config.EmbeddingsServerConfig
if parsedConfig.GetVectorSearch() != nil {
    embeddingsServerCfg = parsedConfig.GetVectorSearch().GetEmbeddingsServer()
}
embeddingsConfig := resolve.ResolveConfig(
    embeddingsServerCfg,
    *resolveEmbeddingsIndexes,
    *embeddingsServerURL,
)

// 6. Instantiate Mixer Server with resolved mappings
mixerServer := server.NewMixerServer(
    ..., 
    embeddingsConfig.ServerURL, 
    embeddingsConfig.DefaultIndex, 
    embeddingsConfig.ResolveIndexMapping,
)
```

---

## Backward Compatibility & Safety

*   **Flag Precedence**: Any CLI flags explicitly set (e.g. `--embeddings_server_url`) will overwrite values loaded from the `mixer_config.yaml` file, preserving existing automated pipelines.
*   **Feature Flag Guard**: The `EnableSpannerSearchEmbeddings` and `EnableEmbeddingsResolver` feature flags remain as the master toggles, overriding configuration values during canary rollouts.

---

## Testing Plan

*   **Config Loader Tests**: Verify `ReadFile` handles local file paths and fetches from GCS successfully.
*   **Resolution Tests**: Verify Spanner and Embeddings resolution logic merges defaults, YAML settings, and CLI flags in the correct precedence order.
*   **Resolver Integration Tests**: Verify index resolution works with dynamically injected index mappings.

---

## GKE / Helm Deployment Integration (CDC Legacy)

For GKE deployments using the Mixer Helm chart, `mixer_config.yaml` is managed as a **Kubernetes ConfigMap** mounted as a local file inside the container.

### 1. Define Config in Helm Values (`values.yaml`)
We add a new key `config` under the `mixer:` block in `values.yaml` to hold the YAML configuration string:

```yaml
# deploy/helm_charts/mixer/values.yaml
mixer:
  ...
  # Inline custom Mixer configuration. If empty, Mixer falls back to defaults.
  config: |
    vector_search:
      embeddings_server:
        resolve_index_mapping:
          multi-entity: "base_multi_entity"
```

### 2. Add to ConfigMaps Template (`config_maps.yaml`)
We mount the configuration string as a file entry `mixer_config.yaml` in the existing `store-config` ConfigMap:

```yaml
# deploy/helm_charts/mixer/templates/config_maps.yaml
kind: ConfigMap
apiVersion: v1
metadata:
  name: {{ include "mixer.fullname" . }}-store-config
data:
  ...
  mixer_config.yaml: {{ .Values.mixer.config | quote }}
```

### 3. Mount in Deployment Template (`deployment.yaml`)
We update the `deployment.yaml` to mount `mixer_config.yaml` into the Mixer container at `/static-config/mixer_config.yaml` and pass the path via the `--config_path` flag:

```yaml
# deploy/helm_charts/mixer/templates/deployment.yaml

# A. Volume Definition
      volumes:
        ...
        - name: static-bt-config-volume
          configMap:
            name: {{ include "mixer.fullname" . }}-store-config
            items:
              - key: base_bigtable_info.yaml
                path: base_bigtable_info.yaml
              # Add this:
              - key: mixer_config.yaml
                path: mixer_config.yaml

# B. Volume Mount & Flag Injection
          command:
            - /bin/sh
            - -c
            - |
              ...
              /go/bin/mixer --base_bigtable_info="$FINAL_BT_CONFIG_CONTENT" \
              ...
              # Add this:
              --config_path="/static-config/mixer_config.yaml" \
          volumeMounts:
            ...
            - name: static-bt-config-volume
              mountPath: /static-config
              readOnly: true
```
This ensures GKE deployments can easily customize Mixer behavior using Helm value overrides without making code changes.

