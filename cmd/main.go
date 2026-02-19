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

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/datacommonsorg/mixer/internal/featureflags"
	logger "github.com/datacommonsorg/mixer/internal/log"
	"github.com/datacommonsorg/mixer/internal/maps"
	"github.com/datacommonsorg/mixer/internal/metrics"
	pbs "github.com/datacommonsorg/mixer/internal/proto/service"
	"github.com/datacommonsorg/mixer/internal/server"
	"github.com/datacommonsorg/mixer/internal/server/cache"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
	"github.com/datacommonsorg/mixer/internal/server/healthcheck"
	"github.com/datacommonsorg/mixer/internal/server/redis"
	"github.com/datacommonsorg/mixer/internal/server/remote"
	"github.com/datacommonsorg/mixer/internal/server/spanner"
	"github.com/datacommonsorg/mixer/internal/server/v3/observation"
	"github.com/datacommonsorg/mixer/internal/sqldb"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"golang.org/x/oauth2/google"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/profiler"
	"google.golang.org/api/compute/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	cbt "cloud.google.com/go/bigtable"
)

var (
	// Server config
	port           = flag.Int("port", 12345, "Port on which to run the server.")
	hostProject    = flag.String("host_project", "", "The GCP project to run the mixer instance.")
	writeUsageLogs = flag.Bool("write_usage_logs", false, "Whether to write usage logs.")
	// BigQuery (Sparql)
	useBigquery      = flag.Bool("use_bigquery", true, "Use Bigquery to serve Sparql Query.")
	bigQueryDataset  = flag.String("bq_dataset", "", "DataCommons BigQuery dataset.")
	schemaPath       = flag.String("schema_path", "", "The directory that contains the schema mapping files")
	bqBillingProject = flag.String("bq_billing_project", "", "The bigquery client project. Query is billed to this project.")
	// Base Bigtable Cache
	useBaseBigtable  = flag.Bool("use_base_bigtable", true, "Use base bigtable cache")
	baseBigtableInfo = flag.String("base_bigtable_info", "", "Yaml formatted text containing information for base Bigtable")
	// Custom Bigtable Cache
	useCustomBigtable  = flag.Bool("use_custom_bigtable", false, "Use custom bigtable cache")
	customBigtableInfo = flag.String("custom_bigtable_info", "", "Yaml formatted text containing information for custom Bigtable")
	// Branch Bigtable Cache
	useBranchBigtable = flag.Bool("use_branch_bigtable", true, "Use branch bigtable cache")
	// SQLite database
	useSQLite  = flag.Bool("use_sqlite", false, "Use SQLite as database.")
	sqlitePath = flag.String("sqlite_path", "", "SQLite database file path.")
	// CloudSQL
	useCloudSQL      = flag.Bool("use_cloudsql", false, "Use Google CloudSQL as database.")
	cloudSQLInstance = flag.String("cloudsql_instance", "", "CloudSQL instance name: e.g. project:region:instance")
	// SQL data path
	// Cache SV/SVG data
	cacheSVG = flag.Bool("cache_svg", true, "Whether to cache stat var (group) info and search index")
	// Include maps client
	useMapsApi = flag.Bool("use_maps_api", true, "Uses maps API for place recognition.")
	// Remote mixer url. The API serves merged data from local and remote mixer
	remoteMixerDomain = flag.String("remote_mixer_domain", "", "Remote mixer domain to fetch and merge data for API response.")
	// Profile startup memory instead of listening for requests
	startupMemoryProfile = flag.String("startup_memprof", "", "File path to write the memory profile of mixer startup to")
	// Serve live profiles of the process (CPU, memory, etc.) over HTTP on this port
	httpProfilePort   = flag.Int("httpprof_port", 0, "Port to serve HTTP profiles from")
	foldRemoteRootSvg = flag.Bool("fold_remote_root_svg", false, "Whether to fold root SVG from remote mixer")
	// Cache map of SV dcid to list of inputPropertyExpressions for StatisticalCalculations
	// TODO: Test Custom DC and set to true after release
	cacheSVFormula = flag.Bool("cache_sv_formula", false, "Whether to cache SV -> inputPropertyExpresions for StatisticalCaclulations.")
	// Spanner Graph
	spannerGraphInfo = flag.String("spanner_graph_info", "", "Yaml formatted text containing information for Spanner Graph.")
	// Redis.
	useRedis  = flag.Bool("use_redis", false, "Use Redis cache.")
	redisInfo = flag.String("redis_info", "", "Yaml formatted text containing information for redis instances.")
	// OpenTelemetry metrics exporter
	metricsExporter = flag.String(
		"metrics_exporter",
		"",
		"Which exporter to use for OpenTelemetry metrics. Valid values are otlp, prometheus, and console (or blank for no-op).",
	)
	featureFlagsPath = flag.String(
		"feature_flags_path",
		"",
		"Path to the feature flags config file.",
	)
	embeddingsServerURL = flag.String(
		"embeddings_server_url",
		"",
		"URL for the embeddings server.",
	)
	resolveEmbeddingsIndexes = flag.String(
		"resolve_embeddings_indexes",
		"",
		"Comma separated list of indexes to use for embeddings resolution.",
	)
)

func main() {
	// Sets up structured logger defaults.
	logger.SetUpLogger()

	slog.Info("Enter mixer main() function")
	// Parse flag
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	var err error
	flags, err := featureflags.NewFlags(*featureFlagsPath)
	if err != nil {
		slog.Error("Failed to create feature flags", "error", err)
		os.Exit(1)
	}
	slog.Info("Created feature flags")

	ctx := context.Background()

	credentials, err := google.FindDefaultCredentials(ctx, compute.ComputeScope)
	if err == nil && credentials.ProjectID != "" {
		cfg := profiler.Config{
			Service:        "datacommons-api",
			ServiceVersion: *baseBigtableInfo + *customBigtableInfo,
		}
		err := profiler.Start(cfg)
		if err != nil {
			slog.Warn("Failed to start profiler", "error", err)
		}
	}

	// Configure metrics exporter.
	if *metricsExporter == "otlp" {
		// Push to an OTLP collector.
		err := metrics.ExportOtlpOverGrpc(ctx)
		if err != nil {
			slog.Error("Failed to start metrics server", "error", err)
			os.Exit(1)
		}
	} else if *metricsExporter == "prometheus" {
		// Serve an HTTP endpoint that can be scraped by Prometheus.
		err := metrics.ExportPrometheusOverHttp()
		if err != nil {
			slog.Error("Failed to start metrics server", "error", err)
			os.Exit(1)
		}
	} else if *metricsExporter == "console" {
		// Print to the console.
		metrics.ExportToConsole()
	} else if *metricsExporter != "" {
		slog.Error("Unknown metrics exporter", "exporter", *metricsExporter)
		os.Exit(1)
	}
	defer func() {
		metrics.ShutdownWithTimeout()
	}()

	// Create grpc server.
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			metrics.InjectMethodNameUnaryInterceptor,
		),
		grpc.ChainStreamInterceptor(
			metrics.InjectMethodNameStreamInterceptor,
		),
	)

	// Data sources.
	sources := []datasource.DataSource{}

	// Spanner Graph.
	if flags.EnableV3 && flags.UseSpannerGraph {
		spannerClient, err := spanner.NewSpannerClient(ctx, *spannerGraphInfo, flags.SpannerGraphDatabase, flags.UseStaleReads)
		if err != nil {
			slog.Error("Failed to create Spanner client", "error", err)
			os.Exit(1)
		}
		if flags.UseStaleReads {
			spannerClient.Start()
		}
		defer spannerClient.Close()
		var ds datasource.DataSource = spanner.NewSpannerDataSource(spannerClient)
		// TODO: Order sources by priority once other implementations are added.
		sources = append(sources, ds)
	}
	slog.Info("After Spanner client creation")

	// Bigtable cache
	var tables []*bigtable.Table
	if *useBaseBigtable {
		baseTables, err := bigtable.CreateBigtables(
			ctx, *baseBigtableInfo, false /*isCustom=*/)
		if err != nil {
			slog.Error("Failed to create base Bigtables", "error", err)
			os.Exit(1)
		}
		tables = append(tables, baseTables...)
	}
	slog.Info("After base bigtable setup")
	if *useCustomBigtable {
		customTables, err := bigtable.CreateBigtables(
			ctx, *customBigtableInfo, true /*isCustom=*/)
		if err != nil {
			slog.Error("Failed to create custom Bigtables", "error", err)
			os.Exit(1)
		}
		// Custom tables ranked highere than base tables.
		tables = append(customTables, tables...)
	}

	// BigQuery
	var bqClient *bigquery.Client
	if *useBigquery {
		if *bqBillingProject == "" {
			*bqBillingProject = *hostProject
		}
		bqClient, err = bigquery.NewClient(ctx, *bqBillingProject)
		if err != nil {
			slog.Error("Failed to create Bigquery client", "error", err)
			os.Exit(1)
		}
	}
	slog.Info("After BQ setup")

	// Branch Bigtable cache
	var branchTableName string
	if *useBranchBigtable {
		branchTableName, err = bigtable.ReadBranchTableName(ctx)
		if err != nil {
			slog.Error("Failed to read branch cache folder", "error", err)
			os.Exit(1)
		}
		btClient, err := cbt.NewClient(
			ctx,
			bigtable.BranchBigtableProject,
			bigtable.BranchBigtableInstance,
		)
		if err != nil {
			slog.Error("Failed to create branch bigtable client", "error", err)
			os.Exit(1)

		}
		branchTable := bigtable.NewBtTable(
			btClient,
			branchTableName,
		)
		tables = append(tables, bigtable.NewTable(branchTableName, branchTable, false /*isCustom=*/))
	}
	slog.Info("After branch setup")

	// Metadata.
	metadata, err := server.NewMetadata(
		ctx,
		*hostProject,
		*bigQueryDataset,
		*schemaPath,
		*remoteMixerDomain,
		*foldRemoteRootSvg,
	)
	if err != nil {
		slog.Error("Failed to create metadata", "error", err)
		os.Exit(1)
	}
	slog.Info("After metadata creation")

	// Remote Mixer.
	// Create remote data source here but don't add it to sources yet since we want it to be the last source added.
	// TODO: clean up how we create and add data sources.
	var remoteDataSource datasource.DataSource
	if flags.EnableV3 && *remoteMixerDomain != "" {
		remoteClient, err := remote.NewRemoteClient(metadata)
		if err != nil {
			slog.Error("Failed to create remote client", "error", err)
			os.Exit(1)
		}
		remoteDataSource = remote.NewRemoteDataSource(remoteClient)
	}
	slog.Info("After remote setup")

	// SQL client
	var sqlClient sqldb.SQLClient
	if *useSQLite {
		client, err := sqldb.NewSQLiteClient(*sqlitePath)
		if err != nil {
			slog.Error("Cannot open sqlite database", "path", *sqlitePath, "error", err)
			os.Exit(1)
		}
		sqlClient.UseConnections(client)
		//nolint:errcheck // TODO: Fix pre-existing issue and remove comment.
		defer sqlClient.Close()
	}

	if *useCloudSQL {
		if sqldb.IsConnected(&sqlClient) {
			slog.Warn("SQL client has already been created, will not use CloudSQL")
		} else {
			client, err := sqldb.NewCloudSQLClient(*cloudSQLInstance)
			if err != nil {
				slog.Error("Cannot open cloud sql database", "instance", *cloudSQLInstance, "error", err)
				os.Exit(1)
			}
			sqlClient.UseConnections(client)
			//nolint:errcheck // TODO: Fix pre-existing issue and remove comment.
			defer sqlClient.Close()
		}
	}

	// Check SQL tables and schema.
	if *useSQLite || *useCloudSQL {
		err = sqlClient.ValidateDatabase()
		if err != nil {
			slog.Error("SQL database validation failed", "error", err)
			os.Exit(1)
		}
	}

	// SQL Data Source
	if flags.EnableV3 && sqldb.IsConnected(&sqlClient) {
		var ds datasource.DataSource = sqldb.NewSQLDataSource(&sqlClient, remoteDataSource)
		sources = append(sources, ds)
	}

	// Store
	if len(tables) == 0 && *remoteMixerDomain == "" && !sqldb.IsConnected(&sqlClient) {
		slog.Error("No bigtables or remote mixer domain or sql database are provided")
		os.Exit(1)
	}
	store, err := store.NewStore(
		bqClient, sqlClient, tables, branchTableName, metadata)
	if err != nil {
		slog.Error("Failed to create a new store", "error", err)
		os.Exit(1)
	}

	// Build the cache that includes stat var group info, stat var search index
	// and custom provenance.
	cacheOptions := cache.CacheOptions{
		FetchSVG:       *cacheSVG,
		SearchSVG:      *cacheSVG,
		CacheSQL:       sqldb.IsConnected(&store.SQLClient),
		CacheSVFormula: *cacheSVFormula,
	}
	c, err := cache.NewCache(ctx, store, cacheOptions, metadata)
	if err != nil {
		slog.Error("Failed to create cache", "error", err)
		os.Exit(1)
	}
	slog.Info("After cache creation")

	// Maps client
	var mapsClient maps.MapsClient
	if *useMapsApi {
		mapsClient, err = maps.NewMapsClient(ctx, metadata.HostProject)
		if err != nil {
			slog.Error("Failed to create Maps client", "error", err)
			os.Exit(1)
		}
	}

	// Add remote data source if it was created.
	if remoteDataSource != nil {
		sources = append(sources, remoteDataSource)
	}

	// DataSources
	dataSources := datasources.NewDataSources(sources)

	// Processors
	processors := []*dispatcher.Processor{}
	if flags.EnableV3 {
		slog.Info("V3 enabled, setting up processors")
		// Mixer in-memory cache.
		dataSourceCache, err := cache.NewDataSourceCache(ctx, dataSources, cacheOptions)
		if err != nil {
			slog.Error("Failed to create data source cache", "error", err)
			os.Exit(1)
		}

		// Cache Processor
		if *useRedis && *redisInfo != "" {
			slog.Info("Setting up Redis cache processor")
			redisClient, err := redis.NewCacheClient(*redisInfo)
			if err != nil {
				slog.Error("Failed to create Redis client", "error", err)
				os.Exit(1)
			}
			//nolint:errcheck // TODO: Fix pre-existing issue and remove comment.
			defer redisClient.Close()

			var redisProcessor dispatcher.Processor = redis.NewCacheProcessor(redisClient)
			processors = append(processors, &redisProcessor)
		}
		slog.Info("After Redis setup")

		// Calculation Processor
		var calculationProcessor dispatcher.Processor = observation.NewCalculationProcessor(dataSources, dataSourceCache.SVFormula(ctx))
		processors = append(processors, &calculationProcessor)
		slog.Info("After calculation processor setup")
	}

	// Dispatcher
	dispatcher := dispatcher.NewDispatcher(processors, dataSources)

	// Create server object
	mixerServer := server.NewMixerServer(store, metadata, c, mapsClient, dispatcher, flags, *writeUsageLogs, *embeddingsServerURL, *resolveEmbeddingsIndexes)
	pbs.RegisterMixerServer(srv, mixerServer)

	// Subscribe to branch cache update
	if *useBranchBigtable {
		err := mixerServer.SubscribeBranchCacheUpdate(ctx)
		if err != nil {
			slog.Error("Failed to subscribe to branch cache update", "error", err)
			os.Exit(1)
		}
	}

	// Register for healthcheck
	healthService := healthcheck.NewHealthChecker()
	grpc_health_v1.RegisterHealthServer(srv, healthService)

	// Gather and write memory profile and quit right before listening for
	// requests
	if *startupMemoryProfile != "" {
		// Code from https://pkg.go.dev/runtime/pprof README
		f, err := os.Create(*startupMemoryProfile)
		if err != nil {
			slog.Error("could not create memory profile", "error", err)
			os.Exit(1)
		}
		//nolint:errcheck // TODO: Fix pre-existing issue and remove comment.
		defer f.Close()
		// explicitly trigger garbage collection to accurately understand memory
		// still in use
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			slog.Error("could not write memory profile", "error", err)
			os.Exit(1)
		}
		return
	}

	// Launch a goroutine that will serve memory requests using net/http/pprof
	if *httpProfilePort != 0 {
		go func() {
			// Code from https://pkg.go.dev/net/http/pprof README
			httpProfileFrom := fmt.Sprintf("localhost:%d", *httpProfilePort)
			slog.Info("Serving profile over HTTP", "address", httpProfileFrom)
			slog.Error("Error serving HTTP profile", "error", http.ListenAndServe(httpProfileFrom, nil))
		}()
	}
	slog.Info("About to listen")
	// Listen on network
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		slog.Error("Failed to listen on network", "error", err)
		os.Exit(1)
	}
	slog.Info("Mixer ready to serve!!")
	if err := srv.Serve(lis); err != nil {
		slog.Error("Failed to serve", "error", err)
		os.Exit(1)
	}
}
