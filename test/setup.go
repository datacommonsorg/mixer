// Copyright 2023 Google LLC
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

package test

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	_ "modernc.org/sqlite" // import the sqlite driver

	"cloud.google.com/go/bigquery"
	"github.com/datacommonsorg/mixer/internal/featureflags"
	"github.com/datacommonsorg/mixer/internal/maps"
	pbs "github.com/datacommonsorg/mixer/internal/proto/service"
	"github.com/datacommonsorg/mixer/internal/server"
	"github.com/datacommonsorg/mixer/internal/server/cache"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
	"github.com/datacommonsorg/mixer/internal/server/remote"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/server/spanner"
	"github.com/datacommonsorg/mixer/internal/server/v3/observation"
	"github.com/datacommonsorg/mixer/internal/sqldb"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/protojson"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
)

// TestOption holds the options for integration test.
type TestOption struct {
	FetchSVG          bool
	SearchSVG         bool
	UseCustomTable    bool
	UseSQLite         bool
	CacheSVFormula    bool
	UseSpannerGraph   bool
	EnableV3          bool
	RemoteMixerDomain string
}

var (
	// LatencyTest is used to check whether to do latency test for import group.
	LatencyTest = os.Getenv("LATENCY_TEST") == "true"
	// GenerateGolden is used to check whether generating golden.
	GenerateGolden = os.Getenv("GENERATE_GOLDEN") == "true"
	// EnableSpannerGraph is used to check whether spanner graph should be enabled.
	// Currently this is only enabled on workstations of developers working on the spanner graph POC.
	// This ensures that the spanner tests don't impact existing tests while in the POC phase.
	// TODO: Remove this variable after POC.
	EnableSpannerGraph = os.Getenv("ENABLE_SPANNER_GRAPH") == "true"
)

// This test runs agains staging staging bt and bq dataset.
// This is billed to GCP project "datcom-ci"
// It needs Application Default Credentials to run locally or need to
// provide service account credential when running on GCP.
const (
	bigqueryBillingProject = "datcom-store"
	hostProject            = "datcom-ci"
)

// Setup creates local server and client.
func Setup(option ...*TestOption) (pbs.MixerClient, func(), error) {
	fetchSVG, searchSVG, useCustomTable, useSQLite, cacheSVFormula, useSpannerGraph, enableV3, remoteMixerDomain := false, false, false, false, false, false, false, ""
	var cacheOptions cache.CacheOptions
	if len(option) == 1 {
		fetchSVG = option[0].FetchSVG
		searchSVG = option[0].SearchSVG
		useCustomTable = option[0].UseCustomTable
		useSQLite = option[0].UseSQLite
		cacheSVFormula = option[0].CacheSVFormula
		cacheOptions.CacheSQL = useSQLite
		cacheOptions.FetchSVG = fetchSVG
		cacheOptions.SearchSVG = searchSVG
		cacheOptions.CacheSVFormula = cacheSVFormula
		useSpannerGraph = option[0].UseSpannerGraph
		enableV3 = option[0].EnableV3
		remoteMixerDomain = option[0].RemoteMixerDomain
	}
	return setupInternal(
		"../deploy/storage/bigquery.version",
		"../deploy/storage/base_bigtable_info.yaml",
		"./custom_bigtable_info.yaml",
		"../deploy/mapping",
		useCustomTable,
		useSQLite,
		useSpannerGraph,
		enableV3,
		cacheOptions,
		remoteMixerDomain,
	)
}

func setupInternal(
	bigqueryVersionFile, baseBigtableInfoYaml, testBigtableInfoYaml, mcfPath string,
	useCustomTable, useSQLite, useSpannerGraph, enableV3 bool,
	cacheOptions cache.CacheOptions,
	remoteMixerDomain string,
) (pbs.MixerClient, func(), error) {
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	bqTableID, _ := os.ReadFile(path.Join(path.Dir(filename), bigqueryVersionFile))
	schemaPath := path.Join(path.Dir(filename), mcfPath)

	// Data sources.
	sources := []datasource.DataSource{}

	var spannerDataSource datasource.DataSource
	var spannerCleanup = func() {}
	if enableV3 && useSpannerGraph {
		spannerClient := NewSpannerClient()
		if spannerClient != nil {
			spannerCleanup = spannerClient.Close
			spannerDataSource = spanner.NewSpannerDataSource(spannerClient)
			// TODO: Order sources by priority once other implementations are added.
			sources = append(sources, spannerDataSource)
		}
	}

	baseBigtableInfo, _ := os.ReadFile(path.Join(path.Dir(filename), baseBigtableInfoYaml))
	tables, err := bigtable.CreateBigtables(ctx, string(baseBigtableInfo), false /*isCustom=*/)
	if err != nil {
		log.Fatalf("failed to create Bigtable tables: %v", err)
	}
	if useCustomTable {
		customBigtableInfo, _ := os.ReadFile(path.Join(path.Dir(filename), testBigtableInfoYaml))
		customTables, err := bigtable.CreateBigtables(ctx, string(customBigtableInfo), true /*isCustom=*/)
		if err != nil {
			log.Fatalf("failed to create Bigtable tables: %v", err)
		}
		tables = append(tables, customTables...)
	}
	// BigQuery.
	bqClient, err := bigquery.NewClient(ctx, bigqueryBillingProject)
	if err != nil {
		log.Fatalf("failed to create Bigquery client: %v", err)
	}
	// SQL client
	var sqlClient sqldb.SQLClient
	if useSQLite {
		client, err := sqldb.NewSQLiteClient(filepath.Join(path.Dir(filename), "./datacommons.db"))
		if err != nil {
			log.Fatalf("Failed to read sqlite database: %v", err)
		}
		sqlClient.UseConnections(client)
		err = sqlClient.ValidateDatabase()
		if err != nil {
			log.Fatalf("SQL database validation failed: %v", err)
		}
		if enableV3 {
			var ds datasource.DataSource = sqldb.NewSQLDataSource(&sqlClient, spannerDataSource)
			sources = append(sources, ds)
		}
	}

	metadata, err := server.NewMetadata(
		ctx,
		hostProject,
		strings.TrimSpace(string(bqTableID)),
		schemaPath,
		remoteMixerDomain,
		false,
	)
	if err != nil {
		return nil, func() {}, err
	}
	st, err := store.NewStore(bqClient, sqlClient, tables, "", metadata)
	if err != nil {
		log.Fatalf("Failed to create a new store: %s", err)
	}
	c, err := cache.NewCache(ctx, st, cacheOptions, metadata)
	if err != nil {
		return nil, func() {}, err
	}
	mapsClient := &maps.FakeMapsClient{}

	if enableV3 && remoteMixerDomain != "" {
		remoteClient, err := remote.NewRemoteClient(metadata)
		if err != nil {
			log.Fatalf("Failed to create remote client: %v", err)
		}
		var ds datasource.DataSource = remote.NewRemoteDataSource(remoteClient)
		sources = append(sources, ds)
	}

	dataSources := datasources.NewDataSources(sources)
	// Processors
	processors := []*dispatcher.Processor{}
	if enableV3 {
		// Mixer in-memory cache.
		dataSourceCache, err := cache.NewDataSourceCache(ctx, dataSources, cacheOptions)
		if err != nil {
			return nil, func() {}, err
		}
		var calculationProcessor dispatcher.Processor = observation.NewCalculationProcessor(dataSources, dataSourceCache.SVFormula(ctx))
		processors = append(processors, &calculationProcessor)
	}

	// Dispatcher
	dispatcher := dispatcher.NewDispatcher(processors, dataSources)

	cleanup := func() {
		spannerCleanup()
	}

	return newClient(st, tables, metadata, c, mapsClient, dispatcher, cleanup)
}

// SetupBqOnly creates local server and client with access to BigQuery only.
func SetupBqOnly() (pbs.MixerClient, func(), error) {
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	bqTableID, _ := os.ReadFile(
		path.Join(path.Dir(filename), "../deploy/storage/bigquery.version"))
	schemaPath := path.Join(path.Dir(filename), "../deploy/mapping/")

	// BigQuery.
	bqClient, err := bigquery.NewClient(ctx, bigqueryBillingProject)
	if err != nil {
		log.Fatalf("failed to create Bigquery client: %v", err)
	}
	metadata, err := server.NewMetadata(
		ctx,
		"",
		strings.TrimSpace(string(bqTableID)),
		schemaPath,
		"",
		false,
	)
	if err != nil {
		return nil, func() {}, err
	}
	st, err := store.NewStore(bqClient, sqldb.SQLClient{}, nil, "", nil)
	if err != nil {
		return nil, func() {}, err
	}
	return newClient(st, nil, metadata, nil, nil, nil, func() {})
}

func newClient(
	mixerStore *store.Store,
	tables []*bigtable.Table,
	metadata *resource.Metadata,
	cachedata *cache.Cache,
	mapsClient maps.MapsClient,
	dispatcher *dispatcher.Dispatcher,
	cleanup func(),
) (pbs.MixerClient, func(), error) {
	flags, err := featureflags.NewFlags("")
	if err != nil {
		return nil, func() {}, err
	}
	// Create mixer server. writeUsageLogs is false by default for tests but is directly tested in handler_v2_test.go
	mixerServer := server.NewMixerServer(mixerStore, metadata, cachedata, mapsClient, dispatcher, flags, false, "", "")
	srv := grpc.NewServer()
	pbs.RegisterMixerServer(srv, mixerServer)
	reflection.Register(srv)
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, func() {}, err
	}
	// Start mixer at localhost:0
	go func() {
		err := srv.Serve(lis)
		if err != nil {
			log.Fatalf("failed to start mixer in localhost:0")
		}
	}()

	// Create mixer client
	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(300000000 /* 300M */)))
	if err != nil {
		return nil, func() {}, err
	}
	mixerClient := pbs.NewMixerClient(conn)
	return mixerClient, cleanup, nil
}

// UpdateGolden updates the golden file for native typed response.
func UpdateGolden(v interface{}, root, fname string) {
	buf := &bytes.Buffer{}
	encoder := json.NewEncoder(buf)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	err := encoder.Encode(v)
	if err != nil {
		log.Printf("could not encode golden response %v", err)
	}
	if os.WriteFile(
		path.Join(root, fname), bytes.TrimRight(buf.Bytes(), "\n"), 0644) != nil {
		log.Printf("could not write golden files to %s", fname)
	}
}

// UpdateProtoGolden updates the golden file for protobuf response.
func UpdateProtoGolden(
	resp protoreflect.ProtoMessage, root string, fname string) {
	var err error
	marshaller := protojson.MarshalOptions{Indent: ""}
	// protojson doesn't and won't make stable output:
	// https://github.com/golang/protobuf/issues/1082
	// Use encoding/json to get stable output.
	data, err := marshaller.Marshal(resp)
	if err != nil {
		log.Printf("marshaller.Marshal(%s) = %s", fname, err)
		return
	}
	var rm json.RawMessage = data
	jsonByte, err := json.MarshalIndent(rm, "", "  ")
	if err != nil {
		log.Printf("json.MarshalIndent(%s) = %s", fname, err)
		return
	}
	err = os.WriteFile(path.Join(root, fname), jsonByte, 0644)
	if err != nil {
		log.Printf("os.WriteFile(%s) = %s", fname, err)
	}
}

// ReadJSON reads in the golden Json file.
func ReadJSON(dir, fname string, resp protoreflect.ProtoMessage) error {
	bytes, err := os.ReadFile(path.Join(dir, fname))
	if err != nil {
		return err
	}
	err = protojson.Unmarshal(bytes, resp)
	if err != nil {
		return err
	}
	return nil
}

// StructToJSON marshals a struct into a json string.
func StructToJSON(
	data interface{}) (string, error) {
	jsonBytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}

// WriteGolden writes a string to a golden file.
func WriteGolden(
	data string, goldenDir string, goldenFile string) error {
	err := os.WriteFile(path.Join(goldenDir, goldenFile), []byte(data), 0644)
	if err != nil {
		return err
	}
	return nil
}

// ReadGolden reads the golden file and returns its content as a string.
func ReadGolden(goldenDir string, goldenFile string) (string, error) {
	bytes, err := os.ReadFile(path.Join(goldenDir, goldenFile))
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// NewSpannerClient creates a new test spanner client if spanner is enabled.
// If not enabled, it returns nil.
func NewSpannerClient() spanner.SpannerClient {
	if !EnableSpannerGraph {
		log.Println("Spanner graph not enabled.")
		return nil
	}
	_, filename, _, _ := runtime.Caller(0)
	spannerGraphInfoYamlPath := path.Join(path.Dir(filename), "../deploy/storage/spanner_graph_info.yaml")
	return newSpannerClient(context.Background(), spannerGraphInfoYamlPath)
}

func newSpannerClient(ctx context.Context, spannerGraphInfoYamlPath string) spanner.SpannerClient {
	spannerGraphInfoYaml, err := os.ReadFile(spannerGraphInfoYamlPath)
	if err != nil {
		log.Fatalf("Failed to read spanner yaml: %v", err)
	}
	// Don't override spannerGraphInfoYaml.database for testing.
	spannerClient, err := spanner.NewSpannerClient(ctx, string(spannerGraphInfoYaml), "", true)
	if err != nil {
		log.Fatalf("Failed to create SpannerClient: %v", err)
	}
	// Use stale reads for testing.
	spannerClient.Start()
	return spannerClient
}
