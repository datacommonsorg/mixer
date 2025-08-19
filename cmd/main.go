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
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"

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
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/oauth2/google"
	"googlemaps.github.io/maps"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/profiler"
	"google.golang.org/api/compute/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	cbt "cloud.google.com/go/bigtable"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
)

var (
	// Server config
	port        = flag.Int("port", 12345, "Port on which to run the server.")
	hostProject = flag.String("host_project", "", "The GCP project to run the mixer instance.")
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
	useSpannerGraph  = flag.Bool("use_spanner_graph", false, "Use Google Spanner as a database.")
	spannerGraphInfo = flag.String("spanner_graph_info", "", "Yaml formatted text containing information for Spanner Graph.")
	// Redis.
	useRedis  = flag.Bool("use_redis", false, "Use Redis cache.")
	redisInfo = flag.String("redis_info", "", "Yaml formatted text containing information for redis instances.")
	// V3 API.
	enableV3 = flag.Bool("enable_v3", false, "Enable datasources in V3 API.")
	// OpenTelemetry metrics exporter
	metricsExporter = flag.String(
		"metrics_exporter",
		"",
		"Which exporter to use for OpenTelemetry metrics. Valid values are otlp, prometheus, and console (or blank for no-op).",
	)
	v3MirrorFraction = flag.Float64(
		"v3_mirror_fraction", 0, "Fraction of V2 API requests to mirror to V3. Value from 0 to 1.0.",
	)
)

func main() {
	log.Println("Enter mixer main() function")
	// Parse flag
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if *v3MirrorFraction < 0 || *v3MirrorFraction > 1.0 {
		log.Fatalf("v3_mirror_fraction must be between 0 and 1.0, got %d", *v3MirrorFraction)
	}
	if *v3MirrorFraction > 0 && !*enableV3 {
		log.Fatalf("v3_mirror_fraction > 0 requires --enable_v3=true")
	}

	ctx := context.Background()
	var err error

	credentials, err := google.FindDefaultCredentials(ctx, compute.ComputeScope)
	if err == nil && credentials.ProjectID != "" {
		cfg := profiler.Config{
			Service:        "datacommons-api",
			ServiceVersion: *baseBigtableInfo + *customBigtableInfo,
		}
		err := profiler.Start(cfg)
		if err != nil {
			log.Printf("Failed to start profiler: %v", err)
		}
	}

	// Configure metrics exporter.
	if *metricsExporter == "otlp" {
		// Push to an OTLP collector.
		err := metrics.ExportOtlpOverGrpc(ctx)
		if err != nil {
			log.Fatalf("Failed to start metrics server: %v", err)
		}
	} else if *metricsExporter == "prometheus" {
		// Serve an HTTP endpoint that can be scraped by Prometheus.
		err := metrics.ExportPrometheusOverHttp()
		if err != nil {
			log.Fatalf("Failed to start metrics server: %v", err)
		}
	} else if *metricsExporter == "console" {
		// Print to the console.
		metrics.ExportToConsole()
	} else if *metricsExporter != "" {
		log.Fatalf("Unknown metrics exporter: %s", *metricsExporter)
	}
	defer func() {
		metrics.ShutdownWithTimeout()
	}()

	// Create grpc server.
	srv := grpc.NewServer(
		// Set up gRPC middleware for per-method gRPC metrics
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			metrics.InjectMethodNameUnaryInterceptor,
		),
		grpc.ChainStreamInterceptor(
			metrics.InjectMethodNameStreamInterceptor,
		),
	)

	// Data sources.
	sources := []*datasource.DataSource{}

	// Spanner Graph.
	if *enableV3 && *useSpannerGraph {
		spannerClient, err := spanner.NewSpannerClient(ctx, *spannerGraphInfo)
		if err != nil {
			log.Fatalf("Failed to create Spanner client: %v", err)
		}
		var ds datasource.DataSource = spanner.NewSpannerDataSource(spannerClient)
		// TODO: Order sources by priority once other implementations are added.
		sources = append(sources, &ds)
	}

	// Bigtable cache
	var tables []*bigtable.Table
	if *useBaseBigtable {
		baseTables, err := bigtable.CreateBigtables(
			ctx, *baseBigtableInfo, false /*isCustom=*/)
		if err != nil {
			log.Fatalf("Failed to create base Bigtables: %v", err)
		}
		tables = append(tables, baseTables...)
	}
	if *useCustomBigtable {
		customTables, err := bigtable.CreateBigtables(
			ctx, *customBigtableInfo, true /*isCustom=*/)
		if err != nil {
			log.Fatalf("Failed to create custom Bigtables: %v", err)
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
			log.Fatalf("Failed to create Bigquery client: %v", err)
		}
	}

	// Branch Bigtable cache
	var branchTableName string
	if *useBranchBigtable {
		branchTableName, err = bigtable.ReadBranchTableName(ctx)
		if err != nil {
			log.Fatalf("Failed to read branch cache folder: %v", err)
		}
		btClient, err := cbt.NewClient(
			ctx,
			bigtable.BranchBigtableProject,
			bigtable.BranchBigtableInstance,
		)
		if err != nil {
			log.Fatalf("Failed to create branch bigtable client: %v", err)

		}
		branchTable := bigtable.NewBtTable(
			btClient,
			branchTableName,
		)
		if err != nil {
			log.Fatalf("Failed to create BigTable client: %v", err)
		}
		tables = append(tables, bigtable.NewTable(branchTableName, branchTable, false /*isCustom=*/))
	}

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
		log.Fatalf("Failed to create metadata: %v", err)
	}

	// Remote Mixer.
	// Create remote data source here but don't add it to sources yet since we want it to be the last source added.
	// TODO: clean up how we create and add data sources.
	var remoteDataSource datasource.DataSource
	if *enableV3 && *remoteMixerDomain != "" {
		remoteClient, err := remote.NewRemoteClient(metadata)
		if err != nil {
			log.Fatalf("Failed to create remote client: %v", err)
		}
		remoteDataSource = remote.NewRemoteDataSource(remoteClient)
	}

	// SQL client
	var sqlClient sqldb.SQLClient
	if *useSQLite {
		client, err := sqldb.NewSQLiteClient(*sqlitePath)
		if err != nil {
			log.Fatalf("Cannot open sqlite database from: %s: %v", *sqlitePath, err)
		}
		sqlClient.UseConnections(client)
		//nolint:errcheck // TODO: Fix pre-existing issue and remove comment.
		defer sqlClient.Close()
	}

	if *useCloudSQL {
		if sqldb.IsConnected(&sqlClient) {
			log.Printf("SQL client has already been created, will not use CloudSQL")
		} else {
			client, err := sqldb.NewCloudSQLClient(*cloudSQLInstance)
			if err != nil {
				log.Fatalf("Cannot open cloud sql database from %s: %v", *cloudSQLInstance, err)
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
			log.Fatalf("SQL database validation failed: %v", err)
		}
	}

	// SQL Data Source
	if *enableV3 && sqldb.IsConnected(&sqlClient) {
		var ds datasource.DataSource = sqldb.NewSQLDataSource(&sqlClient, remoteDataSource)
		sources = append(sources, &ds)
	}

	// Store
	if len(tables) == 0 && *remoteMixerDomain == "" && !sqldb.IsConnected(&sqlClient) {
		log.Fatal("No bigtables or remote mixer domain or sql database are provided")
	}
	store, err := store.NewStore(
		bqClient, sqlClient, tables, branchTableName, metadata)
	if err != nil {
		log.Fatalf("Failed to create a new store: %s", err)
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
		log.Fatalf("Failed to create cache: %v", err)
	}

	// Maps client
	var mapsClient *maps.Client
	if *useMapsApi {
		mapsClient, err = util.MapsClient(ctx, metadata.HostProject)
		if err != nil {
			log.Fatalf("Failed to create Maps client: %v", err)
		}
	}

	// Add remote data source if it was created.
	if remoteDataSource != nil {
		sources = append(sources, &remoteDataSource)
	}

	// DataSources
	dataSources := datasources.NewDataSources(sources)

	// Processors
	processors := []*dispatcher.Processor{}
	if *enableV3 {
		// Cache Processor
		if *useRedis && *redisInfo != "" {
			redisClient, err := redis.NewCacheClient(*redisInfo)
			if err != nil {
				log.Fatalf("Failed to create Redis client: %v", err)
			}
			//nolint:errcheck // TODO: Fix pre-existing issue and remove comment.
			defer redisClient.Close()

			var redisProcessor dispatcher.Processor = redis.NewCacheProcessor(redisClient)
			processors = append(processors, &redisProcessor)
		}

		// Calculation Processor
		var calculationProcessor dispatcher.Processor = observation.NewCalculationProcessor(dataSources, c.SVFormula(ctx))
		processors = append(processors, &calculationProcessor)
	}

	// Dispatcher
	dispatcher := dispatcher.NewDispatcher(processors, dataSources)

	// Create server object
	mixerServer := server.NewMixerServer(store, metadata, c, mapsClient, dispatcher)
	mixerServer.SetV3MirrorFraction(*v3MirrorFraction)
	pbs.RegisterMixerServer(srv, mixerServer)

	// Subscribe to branch cache update
	if *useBranchBigtable {
		err := mixerServer.SubscribeBranchCacheUpdate(ctx)
		if err != nil {
			log.Fatalf("Failed to subscribe to branch cache update: %v", err)
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
			log.Fatalf("could not create memory profile: %s", err)
		}
		//nolint:errcheck // TODO: Fix pre-existing issue and remove comment.
		defer f.Close()
		// explicitly trigger garbage collection to accurately understand memory
		// still in use
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatalf("could not write memory profile: %s", err)
		}
		return
	}

	// Launch a goroutine that will serve memory requests using net/http/pprof
	if *httpProfilePort != 0 {
		go func() {
			// Code from https://pkg.go.dev/net/http/pprof README
			httpProfileFrom := fmt.Sprintf("localhost:%d", *httpProfilePort)
			log.Printf("Serving profile over HTTP on %v", httpProfileFrom)
			log.Printf("%s\n", http.ListenAndServe(httpProfileFrom, nil))
		}()
	}

	// Listen on network
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen on network: %v", err)
	}
	log.Println("Mixer ready to serve!!")
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
