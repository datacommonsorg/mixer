// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// A spanner client wrapper.
package spanner

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/translator/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

// SpannerClient encapsulates the Spanner client.
type SpannerClient interface {
	GetNodeProps(ctx context.Context, ids []string, out bool) (map[string][]*Property, error)
	GetNodeEdgesByID(ctx context.Context, ids []string, arc *v2.Arc, pageSize, offset int) (map[string][]*Edge, error)
	GetObservations(ctx context.Context, variables []string, entities []string, date string) ([]*Observation, error)
	CheckVariableExistence(ctx context.Context, variables []string, entities []string) ([][]string, error)
	CheckVariableSourceExistence(ctx context.Context, variables []string, sources []string, predicate string) ([][]string, error)
	CheckVariableGroupPlaceExistence(ctx context.Context, variableGroups []string, entities []string, predicate string) ([][]string, error)
	GetObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace, date string) ([]*Observation, error)
	SearchNodes(ctx context.Context, query string, types []string) ([]*SearchNode, error)
	ResolveByID(ctx context.Context, nodes []string, in, out string) (map[string][]string, error)
	GetEventCollectionDate(ctx context.Context, placeID, eventType string) ([]string, error)
	GetEventCollectionDcids(ctx context.Context, placeID, eventType, date string) ([]EventIdWithMagnitudeDcid, error)
	GetEventCollection(ctx context.Context, req *pbv1.EventCollectionRequest) (*pbv1.EventCollection, error)
	Sparql(ctx context.Context, nodes []types.Node, queries []*types.Query, opts *types.QueryOptions) ([][]string, error)
	GetProvenanceSummary(ctx context.Context, ids []string) (map[string]map[string]*pb.StatVarSummary_ProvenanceSummary, error)
	GetStatVarGroupNode(ctx context.Context, nodes []string, includeDefinitions bool) ([]*StatVarGroupNode, error)
	GetFilteredStatVarGroupNode(ctx context.Context, nodes []string, constrainedPlaces []string, constrainedImport string, numEntitiesExistence int, includeDefinitions bool) (map[string]*FilteredStatVarGroupNode, error)
	GetFilteredTopic(ctx context.Context, nodes []string, constrainedPlaces []string, constrainedImport string, numEntitiesExistence int) (map[string]int, error)
	VectorSearchQuery(ctx context.Context, tableName string, limit int, embeddings []float64, numLeaves int, threshold float64, nodeTypes []string, embeddingLabel string) ([]*VectorSearchResult, error)
	FilterNodesByTypes(ctx context.Context, nodes []string, typeFilters []string) (map[string][]string, error)
	GetSdmxObservations(ctx context.Context, req *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error)
	GetSdmxAvailability(ctx context.Context, req *sdmxpb.SdmxAvailabilityQuery) (*sdmxpb.SdmxAvailabilityResult, error)
	Id() string
	Start()
	Close()
}

const (
	noChangeLogThreshold = 10 * time.Minute
	failureLogThreshold  = 10 * time.Minute
)

// spannerDatabaseClient encapsulates the Spanner client that directly interacts with the Spanner database.
type spannerDatabaseClient struct {
	client                      *spanner.Client
	containedInPlaceQueryConfig ContainedInPlaceQueryConfig
	timestamp                   atomic.Int64
	ticker                      Ticker
	stopCh                      chan struct{}
	startOnce                   sync.Once
	stopOnce                    sync.Once
	wg                          sync.WaitGroup

	// For mocking in tests.
	updateTimestamp func(context.Context) error

	// Flag to control query logic for IngestionHistory table.
	useNewIngestionHistorySchema bool
	// Flag to control reading from KeyValueStore instead of Cache table.
	useSpannerKeyValueStore bool

	// Logging/State tracking for the timestamp poller.
	tracker *stalenessTracker

	// Flag to dynamically track if the Spanner schema has been initialized.
	dbInitialized atomic.Bool
}

// MultiEntityQueryConfig controls query-planning behavior for the multi-entity schema.
type MultiEntityQueryConfig struct {
	// ContainedInPlaceEntityScanMinVariables is the minimum number of unique
	// requested variables that selects the entity1 range-scan plan for core
	// contained-in-place queries. Zero disables the optimization.
	ContainedInPlaceEntityScanMinVariables int
}

// SpannerClientOptions holds optional configuration settings and feature toggles for SpannerClient.
type SpannerClientOptions struct {
	DatabaseOverride             string
	UseMultiEntitySchema         bool
	UseNewIngestionHistorySchema bool
	UseSpannerKeyValueStore      bool
	ContainedInPlaceQueryConfig  ContainedInPlaceQueryConfig
	MultiEntityQueryConfig       MultiEntityQueryConfig
	SpannerEmulatorCompatibility bool
}

// newSpannerDatabaseClient creates a new spannerDatabaseClient.
func newSpannerDatabaseClient(client *spanner.Client, opts *SpannerClientOptions) (*spannerDatabaseClient, error) {
	if opts == nil {
		opts = &SpannerClientOptions{}
	}
	containedInPlaceQueryConfig, err := validateAndCloneContainedInPlaceQueryConfig(opts.ContainedInPlaceQueryConfig)
	if err != nil {
		return nil, fmt.Errorf("newSpannerDatabaseClient: %w", err)
	}
	sc := &spannerDatabaseClient{
		client:                       client,
		containedInPlaceQueryConfig:  containedInPlaceQueryConfig,
		useNewIngestionHistorySchema: opts.UseNewIngestionHistorySchema,
		useSpannerKeyValueStore:      opts.UseSpannerKeyValueStore,
		tracker:                      newStalenessTracker(noChangeLogThreshold, failureLogThreshold),
	}

	// Set an initial timestamp synchronously before starting the background loop.
	sc.ticker = NewTimestampTicker()
	sc.stopCh = make(chan struct{})
	sc.updateTimestamp = sc.fetchAndUpdateTimestamp
	if err := sc.updateTimestamp(context.Background()); err != nil {
		slog.Warn("Error initializing Spanner staleness timestamp on startup (falling back to default staleness reads)", "error", err.Error())
	}
	return sc, nil
}

// NewRawSpannerClient creates a new SpannerClient without the schema selector.
// This is intended for testing and internal use where a direct client is needed.
func NewRawSpannerClient(ctx context.Context, spannerConfigYaml string, opts *SpannerClientOptions) (SpannerClient, error) {
	if opts == nil {
		opts = &SpannerClientOptions{}
	}
	cfg, err := createSpannerConfig(spannerConfigYaml, opts.DatabaseOverride)
	if err != nil {
		return nil, fmt.Errorf("failed to create spannerDatabaseClient: %w", err)
	}
	client, err := createSpannerClient(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create spannerDatabaseClient: %w", err)
	}
	return newSpannerDatabaseClient(client, opts)
}

// TableConfig holds the names of multi-entity Spanner tables and indexes.
type TableConfig struct {
	TimeSeriesTable              string
	ObservationTable             string
	TimeSeriesByEntity1Index     string
	TimeSeriesByEntity2Index     string
	TimeSeriesByEntity3Index     string
	TimeSeriesByProvenanceIndex  string
	spannerEmulatorCompatibility bool
}

// DefaultTableConfig returns the default suffix-less table and index configuration for multi-entity Spanner tables.
func DefaultTableConfig() TableConfig {
	return TableConfig{
		TimeSeriesTable:             "TimeSeries",
		ObservationTable:            "Observation",
		TimeSeriesByEntity1Index:    "TimeSeriesByEntity1",
		TimeSeriesByEntity2Index:    "TimeSeriesByEntity2",
		TimeSeriesByEntity3Index:    "TimeSeriesByEntity3",
		TimeSeriesByProvenanceIndex: "TimeSeriesByProvenance",
	}
}

func NewSpannerClient(ctx context.Context, spannerConfigYaml string, opts *SpannerClientOptions) (SpannerClient, error) {
	if opts == nil {
		opts = &SpannerClientOptions{}
	}
	rawClient, err := NewRawSpannerClient(ctx, spannerConfigYaml, opts)
	if err != nil {
		return nil, err
	}
	cfg, err := createSpannerConfig(spannerConfigYaml, opts.DatabaseOverride)
	if err != nil {
		return nil, err
	}

	tableCfg := DefaultTableConfig()
	if cfg.TimeSeriesTable != nil {
		tableCfg.TimeSeriesTable = *cfg.TimeSeriesTable
	}
	if cfg.ObservationTable != nil {
		tableCfg.ObservationTable = *cfg.ObservationTable
	}
	if cfg.TimeSeriesByEntity1Index != nil {
		tableCfg.TimeSeriesByEntity1Index = *cfg.TimeSeriesByEntity1Index
	}
	if cfg.TimeSeriesByEntity2Index != nil {
		tableCfg.TimeSeriesByEntity2Index = *cfg.TimeSeriesByEntity2Index
	}
	if cfg.TimeSeriesByEntity3Index != nil {
		tableCfg.TimeSeriesByEntity3Index = *cfg.TimeSeriesByEntity3Index
	}
	if cfg.TimeSeriesByProvenanceIndex != nil {
		tableCfg.TimeSeriesByProvenanceIndex = *cfg.TimeSeriesByProvenanceIndex
	}
	tableCfg.spannerEmulatorCompatibility = opts.SpannerEmulatorCompatibility

	return NewSchemaSelectorClient(rawClient, opts.UseMultiEntitySchema, tableCfg, opts.MultiEntityQueryConfig)
}

// createSpannerClient creates the database name string and initializes the Spanner client.
func createSpannerClient(ctx context.Context, cfg *SpannerConfig) (*spanner.Client, error) {
	// Construct the database name string
	databaseName := fmt.Sprintf("projects/%s/instances/%s/databases/%s", cfg.Project, cfg.Instance, cfg.Database)

	// Create the Spanner client
	client, err := spanner.NewClient(ctx, databaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to create Spanner client: %w", err)
	}

	return client, nil
}

// parseDatabaseURI checks if databaseStr is a full Spanner database URI (projects/P/instances/I/databases/D).
// If it is a full URI, it extracts project, instance, and database.
// If it is not a full URI (e.g., just a database name like "dc_graph_2026_01_27"), it returns empty project/instance and the original string as database.
func parseDatabaseURI(databaseStr string) (project, instance, database string, err error) {
	if !strings.HasPrefix(databaseStr, "projects/") {
		return "", "", databaseStr, nil
	}
	parts := strings.Split(databaseStr, "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "instances" || parts[4] != "databases" || parts[1] == "" || parts[3] == "" || parts[5] == "" {
		return "", "", "", fmt.Errorf("invalid Spanner database URI format: %q (expected projects/<project>/instances/<instance>/databases/<database>)", databaseStr)
	}
	return parts[1], parts[3], parts[5], nil
}

// createSpannerConfig creates the config from the specific yaml string and an optional database override.
func createSpannerConfig(spannerConfigYaml, databaseOverride string) (*SpannerConfig, error) {
	var cfg SpannerConfig
	if err := yaml.Unmarshal([]byte(spannerConfigYaml), &cfg); err != nil {
		return nil, fmt.Errorf("failed to create spanner config: %w", err)
	}

	// Override database config with flag value if set.
	// This is temporary during development to allow fast rollout of version changes.
	// TODO: Once the Spanner instance is stable, revert to using the config.
	if databaseOverride != "" {
		proj, inst, db, err := parseDatabaseURI(databaseOverride)
		if err != nil {
			return nil, err
		}
		if proj != "" {
			slog.Debug("Setting Spanner project value from database URI flag", "value", proj)
			cfg.Project = proj
		}
		if inst != "" {
			slog.Debug("Setting Spanner instance value from database URI flag", "value", inst)
			cfg.Instance = inst
		}
		slog.Debug("Setting Spanner database value from flag", "value", db)
		cfg.Database = db
	}

	return &cfg, nil
}

func (sc *spannerDatabaseClient) Id() string {
	return sc.client.DatabaseName()
}

// Start starts the background goroutine to periodically fetch the timestamp.
func (sc *spannerDatabaseClient) Start() {
	sc.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())

		sc.wg.Add(1)
		go func() {
			// Defer statements are processed in LIFO order.
			// Mark the wait group as done.
			defer sc.wg.Done()
			// Cancel the context to clean up any in-flight operations.
			defer cancel()
			// Stop the ticker.
			defer sc.ticker.Stop()

			for {
				select {
				case <-sc.stopCh:
					return
				case <-sc.ticker.C():
					// Ignore the error here to allow the process to continue running
					// even if one fetch fails. The previous timestamp remains in cache.
					now := time.Now()
					err := sc.updateTimestamp(ctx)
					if err != nil {
						slog.Error("Error updating Spanner staleness timestamp", "error", err)
						if sc.tracker != nil {
							if ev := sc.tracker.RecordFailure(now, err); ev != nil {
								slog.Log(ctx, ev.level, ev.message, ev.args...)
							}
						}
					}
				}
			}
		}()
	})
}

// Close closes the Spanner client and stops the background goroutine.
func (sc *spannerDatabaseClient) Close() {
	sc.stopOnce.Do(func() {
		close(sc.stopCh)

		sc.wg.Wait()

		if sc.client != nil {
			sc.client.Close()
		}
	})
}

// GetSdmxObservations is not supported on the default client.
func (sc *spannerDatabaseClient) GetSdmxObservations(ctx context.Context, req *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error) {
	return nil, status.Error(codes.Unimplemented, "SDMX queries are only supported on the multi-entity schema")
}

// GetSdmxAvailability is not supported on the default client.
func (sc *spannerDatabaseClient) GetSdmxAvailability(ctx context.Context, req *sdmxpb.SdmxAvailabilityQuery) (*sdmxpb.SdmxAvailabilityResult, error) {
	return nil, status.Error(codes.Unimplemented, "SDMX availability is only supported on the multi-entity schema")
}
