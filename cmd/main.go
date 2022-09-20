// Copyright 2020 Google LLC
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

	pb "github.com/datacommonsorg/mixer/internal/proto"
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
	"google.golang.org/grpc/credentials/alts"
	"google.golang.org/grpc/health/grpc_health_v1"
)

var (
	// Server config
	port         = flag.Int("port", 12345, "Port on which to run the server.")
	useALTS      = flag.Bool("use_alts", false, "Whether to use ALTS server authentication")
	mixerProject = flag.String("mixer_project", "", "The GCP project to run the mixer instance.")
	// Bigtable and BigQuery project
	storeProject = flag.String("store_project", "", "GCP project stores Bigtable and BigQuery.")
	// BigQuery (Sparql)
	useBigquery = flag.Bool("use_bigquery", true, "Use Bigquery to serve Sparql Query")
	bqDataset   = flag.String("bq_dataset", "", "DataCommons BigQuery dataset.")
	schemaPath  = flag.String("schema_path", "", "The directory that contains the schema mapping files")
	// Base Bigtable Cache
	useBaseBt         = flag.Bool("use_base_bt", true, "Use base bigtable cache")
	importGroupTables = flag.String("import_group_tables", "", "Newline separated list of import group tables")
	// Branch Bigtable Cache
	useBranchBt = flag.Bool("use_branch_bt", true, "Use branch bigtable cache")
	// GCS to hold memdb data.
	// Note GCS bucket and pubsub should be within the mixer project.
	useTmcfCsvData = flag.Bool("use_tmcf_csv_data", false, "Use tmcf and csv data")
	tmcfCsvBucket  = flag.String("tmcf_csv_bucket", "", "The GCS bucket that contains tmcf and csv files")
	tmcfCsvFolder  = flag.String("tmcf_csv_folder", "", "GCS folder for an import. An import must have a unique prefix within a bucket.")
	memdbPath      = flag.String("memdb_path", "", "File path of memdb config")
	// Specify what services to serve
	serveMixerService = flag.Bool("serve_mixer_service", true, "Serve Mixer service")
	serveReconService = flag.Bool("serve_recon_service", false, "Serve Recon service")
	// Profile startup memory instead of listening for requests
	startupMemoryProfile = flag.String("startup_memprof", "", "File path to write the memory profile of mixer startup to")
	// Serve live profiles of the process (CPU, memory, etc.) over HTTP on this port
	httpProfilePort = flag.Int("httpprof_port", 0, "Port to serve HTTP profiles from")
)

const (
	// Base BigTable
	baseBtInstance = "prophet-cache"
	// Branch BigTable and Pubsub
	branchBtInstance            = "prophet-branch-cache"
	branchCacheVersionBucket    = "datcom-control"
	branchCacheSubscriberPrefix = "branch-cache-subscriber-"
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
			ServiceVersion: *importGroupTables,
		}
		err := profiler.Start(cfg)
		if err != nil {
			log.Printf("Failed to start profiler: %v", err)
		}
	}

	opts := []grpc.ServerOption{}
	// Use ALTS server credential to bind to VM's private IPv6 interface.
	if *useALTS {
		altsTC := alts.NewServerCreds(alts.DefaultServerOptions())
		opts = append(opts, grpc.Creds(altsTC))
	}

	// Create grpc server.
	srv := grpc.NewServer(opts...)

	branchCachePubsubTopic := "proto-branch-cache-reload"
	branchCacheVersionFile := "latest_proto_branch_cache_version.txt"

	// Base Bigtable cache
	var tables []*bigtable.Table
	if *useBaseBt {
		tableNames := util.ParseBigtableGroup(*importGroupTables)
		for _, name := range tableNames {
			t, err := bigtable.NewBtTable(ctx, *storeProject, baseBtInstance, name)
			if err != nil {
				log.Fatalf("Failed to create BigTable client: %v", err)
			}
			tables = append(tables, bigtable.NewTable(name, t))
		}
	}

	if *serveMixerService {
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

		// BigQuery.
		var bqClient *bigquery.Client
		if *useBigquery {
			bqClient, err = bigquery.NewClient(ctx, *mixerProject)
			if err != nil {
				log.Fatalf("Failed to create Bigquery client: %v", err)
			}
		}

		// Branch Bigtable cache
		var branchTableName string
		if *useBranchBt {
			branchTableName, err = server.ReadBranchTableName(
				ctx, branchCacheVersionBucket, branchCacheVersionFile)
			if err != nil {
				log.Fatalf("Failed to read branch cache folder: %v", err)
			}
			branchTable, err := bigtable.NewBtTable(ctx, *storeProject, branchBtInstance, branchTableName)
			if err != nil {
				log.Fatalf("Failed to create BigTable client: %v", err)
			}
			tables = append(tables, bigtable.NewTable(branchTableName, branchTable))
		}

		// Metadata.
		metadata, err := server.NewMetadata(
			*mixerProject, *bqDataset, *storeProject, branchBtInstance, *schemaPath)
		if err != nil {
			log.Fatalf("Failed to create metadata: %v", err)
		}

		// Store
		store := store.NewStore(bqClient, memDb, tables, branchTableName, metadata)
		// Build the cache that includes stat var group info and stat var search
		// Index.
		// !!Important: do this after creating the memdb, since the cache will
		// need to merge svg info from memdb.
		var cache *resource.Cache
		if *serveMixerService {
			cache, err = server.NewCache(ctx, store, server.SearchOptions{
				UseSearch:           true,
				BuildSvgSearchIndex: true,
				BuildSqliteIndex:    true,
			})
			if err != nil {
				log.Fatalf("Failed to create cache: %v", err)
			}
		}

		// Create server object
		mixerServer := server.NewMixerServer(store, metadata, cache)
		pb.RegisterMixerServer(srv, mixerServer)

		// Subscribe to branch cache update
		if *useBranchBt {
			err := mixerServer.SubscribeBranchCacheUpdate(ctx, *storeProject,
				branchCacheSubscriberPrefix, branchCachePubsubTopic)
			if err != nil {
				log.Fatalf("Failed to subscribe to branch cache update: %v", err)
			}
		}
	}

	// Register for Recon Service.
	if *serveReconService {
		store := store.NewStore(nil, nil, tables, "", nil)
		reconServer := server.NewReconServer(store)
		pb.RegisterReconServer(srv, reconServer)
	}

	// Register for healthcheck.
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
