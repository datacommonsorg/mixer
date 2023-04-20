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
	"path"
	"runtime"
	"runtime/pprof"

	pbs "github.com/datacommonsorg/mixer/internal/proto/service"
	"github.com/datacommonsorg/mixer/internal/server"
	"github.com/datacommonsorg/mixer/internal/server/healthcheck"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/store/memdb"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/oauth2/google"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/profiler"
	"google.golang.org/api/compute/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

var (
	// Server config
	port        = flag.Int("port", 12345, "Port on which to run the server.")
	hostProject = flag.String("host_project", "", "The GCP project to run the mixer instance.")
	// BigQuery (Sparql)
	useBigquery      = flag.Bool("use_bigquery", true, "Use Bigquery to serve Sparql Query.")
	bqDataset        = flag.String("bq_dataset", "", "DataCommons BigQuery dataset.")
	schemaPath       = flag.String("schema_path", "", "The directory that contains the schema mapping files")
	bqBillingProject = flag.String("bq_billing_project", "", "The bigquery client project. Query is billed to this project.")
	// Base Bigtable Cache
	useBaseBt          = flag.Bool("use_base_bt", true, "Use base bigtable cache")
	baseBigtableInfo   = flag.String("base_bigtable_info", "", "Yaml formatted text containing information for base Bigtable")
	customBigtableInfo = flag.String("custom_bigtable_info", "", "Yaml formatted text containing information for custom Bigtable")
	// Branch Bigtable Cache
	useBranchBt = flag.Bool("use_branch_bt", true, "Use branch bigtable cache")
	// Stat-var search cache
	useSearch = flag.Bool("use_search", true, "Uses stat var search. Will build search indexes.")
	// GCS to hold memdb data.
	// Note GCS bucket and pubsub should be within the mixer project.
	useTmcfCsvData = flag.Bool("use_tmcf_csv_data", false, "Use tmcf and csv data")
	tmcfCsvBucket  = flag.String("tmcf_csv_bucket", "", "The GCS bucket that contains tmcf and csv files")
	tmcfCsvFolder  = flag.String("tmcf_csv_folder", "", "GCS folder for an import. An import must have a unique prefix within a bucket.")
	memdbPath      = flag.String("memdb_path", "", "File path of memdb config")
	// Remote mixer url. The API serves merged data from local and remote mixer
	remoteMixerDomain = flag.String("remote_mixer_domain", "", "Remote mixer domain to fetch and merge data for API response.")
	// Profile startup memory instead of listening for requests
	startupMemoryProfile = flag.String("startup_memprof", "", "File path to write the memory profile of mixer startup to")
	// Serve live profiles of the process (CPU, memory, etc.) over HTTP on this port
	httpProfilePort = flag.Int("httpprof_port", 0, "Port to serve HTTP profiles from")
)

const (
	// Memdb config file name
	memdbConfig = "memdb.json"
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
	if *useBaseBt {
		baseTables, err := bigtable.CreateBigtables(
			ctx, *baseBigtableInfo, false /*isCustom=*/)
		if err != nil {
			log.Fatalf("Failed to create base Bigtables: %v", err)
		}
		customTables, err := bigtable.CreateBigtables(
			ctx, *customBigtableInfo, true /*isCustom=*/)
		if err != nil {
			log.Fatalf("Failed to create custom Bigtables: %v", err)
		}
		// Custom tables ranked highere than base tables.
		tables = append(customTables, baseTables...)
	}

	// TMCF + CSV from GCS
	memDb := memdb.NewMemDb()
	if *useTmcfCsvData && *tmcfCsvBucket != "" {
		// Read memdb config
		err = memDb.LoadConfig(ctx, path.Join(*memdbPath, memdbConfig))
		if err != nil {
			log.Fatalf("Failed to load config: %v", err)
		}
		err = memDb.LoadFromGcs(ctx, *tmcfCsvBucket, *tmcfCsvFolder)
		if err != nil {
			log.Fatalf("Failed to load tmcf and csv from GCS: %v", err)
		}
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
	if *useBranchBt {
		branchTableName, err = bigtable.ReadBranchTableName(ctx)
		if err != nil {
			log.Fatalf("Failed to read branch cache folder: %v", err)
		}
		branchTable, err := bigtable.NewBtTable(
			ctx,
			bigtable.BranchBigtableProject,
			bigtable.BranchBigtableInstance,
			branchTableName,
		)
		if err != nil {
			log.Fatalf("Failed to create BigTable client: %v", err)
		}
		tables = append(tables, bigtable.NewTable(branchTableName, branchTable, false /*isCustom=*/))
	}

	// Metadata.
	metadata, err := server.NewMetadata(
		*hostProject,
		*bqDataset,
		*schemaPath,
		*remoteMixerDomain,
	)
	if err != nil {
		log.Fatalf("Failed to create metadata: %v", err)
	}

	// Store
	if len(tables) == 0 && *remoteMixerDomain == "" {
		log.Fatal("No bigtables or remote mixer domain are provided")
	}
	store, err := store.NewStore(bqClient, memDb, tables, branchTableName, metadata)
	if err != nil {
		log.Fatalf("Failed to create a new store: %s", err)
	}
	// Build the cache that includes stat var group info and stat var search
	// Index.
	// !!Important: do this after creating the memdb, since the cache will
	// need to merge svg info from memdb.
	var cache *resource.Cache
	if *useSearch {
		cache, err = server.NewCache(ctx, store, server.SearchOptions{
			UseSearch:           true,
			BuildSvgSearchIndex: true,
			BuildSqliteIndex:    true,
		})
		if err != nil {
			log.Fatalf("Failed to create cache: %v", err)
		}
	}

	// Maps client
	mapsClient, err := util.MapsClient(ctx, metadata.HostProject)
	if err != nil {
		log.Fatalf("Failed to create Maps client: %v", err)
	}

	// Create server object
	mixerServer := server.NewMixerServer(store, metadata, cache, mapsClient)
	pbs.RegisterMixerServer(srv, mixerServer)

	// Subscribe to branch cache update
	if *useBranchBt {
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
