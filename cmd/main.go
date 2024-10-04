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
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"

	pbs "github.com/datacommonsorg/mixer/internal/proto/service"
	"github.com/datacommonsorg/mixer/internal/server"
	"github.com/datacommonsorg/mixer/internal/server/cache"
	"github.com/datacommonsorg/mixer/internal/server/healthcheck"
	"github.com/datacommonsorg/mixer/internal/sqldb"
	"github.com/datacommonsorg/mixer/internal/sqldb/cloudsql"
	"github.com/datacommonsorg/mixer/internal/sqldb/sqlite"
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
	_ "modernc.org/sqlite" // import the sqlite driver
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
)

func main() {
	log.Println("Enter mixer main() function")
	// Parse flag
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)

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

	// Create grpc server.
	srv := grpc.NewServer()

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

	// SQLite DB
	var sqlClient *sql.DB
	if *useSQLite {
		sqlClient, err = sqlite.ConnectDB(*sqlitePath)
		if err != nil {
			log.Fatalf("Cannot open sqlite database from: %s: %v", *sqlitePath, err)
		}
		defer sqlClient.Close()
	}

	if *useCloudSQL {
		if sqlClient != nil {
			log.Printf("SQL client has already been created, will not use CloudSQL")
		} else {
			sqlClient, err = cloudsql.ConnectWithConnector(*cloudSQLInstance)
			if err != nil {
				log.Fatalf("Cannot open cloud sql database from %s: %v", *cloudSQLInstance, err)
			}
			defer sqlClient.Close()
		}
	}

	// Create tables for new database.
	if *useSQLite || *useCloudSQL {
		if err := sqldb.CreateTables(sqlClient); err != nil {
			log.Fatalf("Can not create tables in database: %v", err)
		}

		err = sqldb.CheckSchema(sqlClient)
		if err != nil {
			log.Fatalf("SQL schema check failed: %v", err)
		}
	}

	// Store
	if len(tables) == 0 && *remoteMixerDomain == "" && sqlClient == nil {
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
		CacheSQL:       store.SQLClient != nil,
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

	// Create server object
	mixerServer := server.NewMixerServer(store, metadata, c, mapsClient)
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
