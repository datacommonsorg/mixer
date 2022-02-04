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
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"

	cbt "cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server"
	"github.com/datacommonsorg/mixer/internal/server/healthcheck"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/store/memdb"
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
	baseTableName     = flag.String("base_table", "", "Base cache Bigtable table.")
	useImportGroup    = flag.Bool("use_import_group", false, "Use multiple base tables from import group")
	importGroupTables = flag.String("import_group_tables", "", "A JSON string of import group tables")
	// Branch Bigtable Cache
	useBranchBt = flag.Bool("use_branch_bt", true, "Use branch bigtable cache")
	// GCS to hold memdb data.
	// Note GCS bucket and pubsub should be within the mixer project.
	useTmcfCsvData = flag.Bool("use_tmcf_csv_data", false, "Use tmcf and csv data")
	tmcfCsvBucket  = flag.String("tmcf_csv_bucket", "", "The GCS bucket that contains tmcf and csv files")
	tmcfCsvFolder  = flag.String("tmcf_csv_folder", "", "GCS folder for an import. An import must have a unique prefix within a bucket.")
	// Specify what services to serve
	serveMixerService = flag.Bool("serve_mixer_service", true, "Serve Mixer service")
	serveReconService = flag.Bool("serve_recon_service", false, "Serve Recon service")
)

const (
	// Base BigTable
	baseBtInstance = "prophet-cache"
	// Branch BigTable and Pubsub
	branchBtInstance            = "prophet-branch-cache"
	branchCacheVersionBucket    = "datcom-control"
	branchCacheVersionFile      = "latest_branch_cache_version.txt"
	branchCachePubsubTopic      = "branch-cache-reload"
	branchCacheSubscriberPrefix = "branch-cache-subscriber-"
	// GCS Pubsub
	tmcfCsvPubsubTopic      = "tmcf-csv-reload"
	tmcfCsvSubscriberPrefix = "tmcf-csv-subscriber-"
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
			ServiceVersion: *baseTableName,
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

	// Metadata.
	metadata, err := server.NewMetadata(*bqDataset, *storeProject, branchBtInstance, *schemaPath)
	if err != nil {
		log.Fatalf("Failed to create metadata: %v", err)
	}

	// Base Bigtable cache
	var baseTables []*cbt.Table
	if *useBaseBt {
		if *useImportGroup {
			var c bigtable.TableConfig
			if err := json.Unmarshal([]byte(*importGroupTables), &c); err != nil {
				log.Fatalf("Failed to load import group tables config")
			}
			tableNames := c.Tables
			bigtable.SortTables(tableNames)
			for _, t := range tableNames {
				baseTable, err := bigtable.NewTable(ctx, *storeProject, baseBtInstance, t)
				if err != nil {
					log.Fatalf("Failed to create BigTable client: %v", err)
				}
				baseTables = append(baseTables, baseTable)
				metadata.BaseTables = append(metadata.BaseTables, t)
			}

		} else {
			// Base cache
			baseTable, err := bigtable.NewTable(ctx, *storeProject, baseBtInstance, *baseTableName)
			if err != nil {
				log.Fatalf("Failed to create BigTable client: %v", err)
			}
			baseTables = append(baseTables, baseTable)
			metadata.BaseTables = []string{*baseTableName}
		}
	}

	if *serveMixerService {
		// TMCF + CSV from GCS
		memDb := memdb.NewMemDb()
		if *useTmcfCsvData && *tmcfCsvBucket != "" {
			err = memDb.LoadFromGcs(ctx, *tmcfCsvBucket, *tmcfCsvFolder)
			if err != nil {
				log.Fatalf("Failed to load tmcf and csv from GCS: %v", err)
			}
			err = memDb.SubscribeGcsUpdate(
				ctx, *mixerProject, tmcfCsvPubsubTopic, tmcfCsvSubscriberPrefix,
				*tmcfCsvBucket, *tmcfCsvFolder)
			if err != nil {
				log.Fatalf("Failed to subscribe to tmcf and csv change: %v", err)
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
		var branchTable *cbt.Table
		if *useBranchBt {
			branchTableName, err := server.ReadBranchTableName(
				ctx, branchCacheVersionBucket, branchCacheVersionFile)
			if err != nil {
				log.Fatalf("Failed to read branch cache folder: %v", err)
			}
			branchTable, err = bigtable.NewTable(ctx, *storeProject, branchBtInstance, branchTableName)
			if err != nil {
				log.Fatalf("Failed to create BigTable client: %v", err)
			}
			metadata.BranchTable = branchTableName
		}

		// Store
		store := store.NewStore(bqClient, memDb, baseTables, branchTable)
		// Cache.
		var cache *resource.Cache
		if *serveMixerService {
			cache, err = server.NewCache(ctx, store)
			if err != nil {
				log.Fatalf("Failed to create cache: %v", err)
			}
		}

		// Create server object
		mixerServer := server.NewMixerServer(store, metadata, cache)
		pb.RegisterMixerServer(srv, mixerServer)

		// Subscribe to branch cache update
		if *useBranchBt {
			err := mixerServer.SubscribeBranchCacheUpdate(
				ctx, *storeProject, branchCacheSubscriberPrefix, branchCachePubsubTopic)
			if err != nil {
				log.Fatalf("Failed to subscribe to branch cache update: %v", err)
			}
		}
	}

	// Register for Recon Service.
	if *serveReconService {
		reconServer := server.NewReconServer(baseTables)
		pb.RegisterReconServer(srv, reconServer)
	}

	// Register for healthcheck.
	healthService := healthcheck.NewHealthChecker()
	grpc_health_v1.RegisterHealthServer(srv, healthService)

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
