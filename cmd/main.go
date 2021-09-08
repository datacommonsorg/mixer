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
	"os"
	"os/signal"
	"syscall"

	"github.com/datacommonsorg/mixer/internal/healthcheck"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server"
	"github.com/datacommonsorg/mixer/internal/store"
	"golang.org/x/oauth2/google"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/profiler"
	"google.golang.org/api/compute/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/alts"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

var (
	mixerProject    = flag.String("mixer_project", "", "The cloud project to run the mixer instance.")
	storeProject    = flag.String("store_project", "", "GCP project stores Bigtable and BigQuery.")
	bqDataset       = flag.String("bq_dataset", "", "DataCommons BigQuery dataset.")
	baseTableName   = flag.String("base_table", "", "Base cache Bigtable table.")
	port            = flag.Int("port", 12345, "Port on which to run the server.")
	useALTS         = flag.Bool("use_alts", false, "Whether to use ALTS server authentication")
	schemaPath      = flag.String("schema_path", "", "The directory that contains the schema mapping files")
	gcsBucket       = flag.String("gcs_bucket", "", "The GCS bucket that contains tmcf and csv files")
	gcsObjectPrefix = flag.String("gcs_object_prefix", "", "Path (sub-directory) for an import. An import must have a unique prefix within a bucket.")
	useBigquery     = flag.Bool("use_bigquery", true, "Use Bigquery to serve Sparql Query")
	useBaseBt       = flag.Bool("use_base_bt", true, "Use base bigtable cache")
	useBranchBt     = flag.Bool("use_branch_bt", true, "Use branch bigtable cache")
	useGcsData      = flag.Bool("use_gcs_data", false, "Use tmcf and csv from GCS")
)

const (
	baseBtInstance           = "prophet-cache"
	branchBtInstance         = "prophet-branch-cache"
	branchCacheVersionFile   = "latest_branch_cache_version.txt"
	pubsubTopic              = "branch-cache-reload"
	subscriberPrefix         = "mixer-subscriber-"
	branchCacheVersionBucket = "datcom-control"
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
			Service:        "mixer-service",
			ServiceVersion: *baseTableName,
		}
		err := profiler.Start(cfg)
		if err != nil {
			log.Printf("Failed to start profiler: %v", err)
		}
	}

	// TMCF + CSV from GCS
	memdb := store.NewMemDb()
	if *useGcsData && *gcsBucket != "" {
		err = memdb.LoadFromGcs(ctx, *gcsBucket, *gcsObjectPrefix)
		if err != nil {
			log.Fatalf("Failed to load tmcf and csv from GCS: %v", err)
		}
	}

	// BigQuery.
	var bqClient *bigquery.Client
	var metadata *server.Metadata
	if *useBigquery {
		bqClient, err = bigquery.NewClient(ctx, *mixerProject)
		if err != nil {
			log.Fatalf("Failed to create Bigquery client: %v", err)
		}
		// Metadata.
		metadata, err = server.NewMetadata(*bqDataset, *storeProject, branchBtInstance, *schemaPath)
		if err != nil {
			log.Fatalf("Failed to create metadata: %v", err)
		}
	}

	// Base Bigtable cache
	var baseTable *bigtable.Table
	var cache *server.Cache
	if *useBaseBt {
		// Base cache
		baseTable, err = server.NewBtTable(ctx, *storeProject, baseBtInstance, *baseTableName)
		if err != nil {
			log.Fatalf("Failed to create BigTable client: %v", err)
		}
		// Cache.
		cache, err = server.NewCache(ctx, baseTable)
		if err != nil {
			log.Fatalf("Failed to create cache: %v", err)
		}
	}

	// Branch Bigtable cache
	var branchTable *bigtable.Table
	if *useBranchBt {
		branchTableName, err := server.ReadBranchTableName(
			ctx, branchCacheVersionBucket, branchCacheVersionFile)
		if err != nil {
			log.Fatalf("Failed to read branch cache folder: %v", err)
		}
		branchTable, err = server.NewBtTable(ctx, *storeProject, branchBtInstance, branchTableName)
		if err != nil {
			log.Fatalf("Failed to create BigTable client: %v", err)
		}
	}

	// Create server object
	s := server.NewServer(bqClient, baseTable, branchTable, metadata, cache, memdb)

	// Subscribe to branch cache update
	if *useBranchBt {
		sub, err := s.SubscribeBranchCacheUpdate(
			ctx, *storeProject, branchCacheVersionBucket, subscriberPrefix, pubsubTopic)
		if err != nil {
			log.Fatalf("Failed to subscribe to branch cache update: %v", err)
		}
		// Create a go routine to check server shutdown and delete the subscriber.
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-c
			err := sub.Delete(ctx)
			if err != nil {
				log.Fatalf("Failed to delete subscriber: %v", err)
			}
			log.Printf("Deleted subscriber: %v", sub)
			os.Exit(1)
		}()
	}

	opts := []grpc.ServerOption{}

	// Use ALTS server credential to bind to VM's private IPv6 interface.
	if *useALTS {
		altsTC := alts.NewServerCreds(alts.DefaultServerOptions())
		opts = append(opts, grpc.Creds(altsTC))
	}

	// Start mixer
	srv := grpc.NewServer(opts...)
	pb.RegisterMixerServer(srv, s)
	// Register reflection service on gRPC server.
	reflection.Register(srv)

	healthService := healthcheck.NewHealthChecker()
	grpc_health_v1.RegisterHealthServer(srv, healthService)

	// Listen on network
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen on network: %v", err)
	}
	fmt.Println("Mixer ready to serve!!")
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
