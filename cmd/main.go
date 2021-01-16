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

	"github.com/datacommonsorg/mixer/internal/healthcheck"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server"
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
	bqDataset      = flag.String("bq_dataset", "", "DataCommons BigQuery dataset.")
	btProject      = flag.String("bt_project", "", "GCP project containing the BigTable instance.")
	baseInstance   = flag.String("base_instance", "", "Base cache BigTable instance.")
	baseTableName  = flag.String("base_table", "", "Base cache Bigtable table.")
	branchInstance = flag.String("branch_instance", "", "Branch cache BigTable instance.")
	projectID      = flag.String("project_id", "", "The cloud project to run the mixer instance.")
	port           = flag.Int("port", 12345, "Port on which to run the server.")
	useALTS        = flag.Bool("use_alts", false, "Whether to use ALTS server authentication")
	bigqueryOnly   = flag.Bool("bigquery_only", false, "The service only serves sparql query")
)

const (
	branchCacheVersionBucket = "prophet_cache"
	branchCacheVersionFile   = "latest_branch_cache_version.txt"
	pubsubProject            = "google.com:datcom-store-dev"
	pubsubTopic              = "branch-cache-reload"
	subscriberPrefix         = "mixer-subscriber-"
)

func main() {
	fmt.Println("Enter mixer main() function")

	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	ctx := context.Background()

	credentials, error := google.FindDefaultCredentials(ctx, compute.ComputeScope)
	if error == nil && credentials.ProjectID != "" {
		cfg := profiler.Config{
			Service:        "mixer-service",
			ServiceVersion: *baseTableName,
		}
		err := profiler.Start(cfg)
		if err != nil {
			log.Printf("Failed to start profiler: %v", err)
		}
	}

	// BigQuery.
	bqClient, err := bigquery.NewClient(ctx, *projectID)
	if err != nil {
		log.Fatalf("Failed to create Bigquery client: %v", err)
	}

	btTables := []*bigtable.Table{}
	var btClient *bigtable.Client
	if !*bigqueryOnly {
		// Base cache
		_, baseTable, err := server.NewBtTable(ctx, *btProject, *baseInstance, *baseTableName)
		if err != nil {
			log.Fatalf("Failed to create BigTable client: %v", err)
		}
		branchTableName, err := server.ReadBranchTableName(
			ctx, branchCacheVersionBucket, branchCacheVersionFile)
		if err != nil {
			log.Fatalf("Failed to read branch cache folder: %v", err)
		}
		branchClient, branchTable, err := server.NewBtTable(ctx, *btProject, *branchInstance, branchTableName)
		btClient = branchClient
		if err != nil {
			log.Fatalf("Failed to create BigTable client: %v", err)
		}
		// Order matters, insert branchTable first so the branch cache data is preferred.
		btTables = append(btTables, branchTable)
		btTables = append(btTables, baseTable)
	}

	// Metadata.
	metadata, err := server.NewMetadata(*bqDataset, *btProject, *branchInstance)
	if err != nil {
		log.Fatalf("Failed to create metadata: %v", err)
	}

	// Create server object
	s := server.NewServer(bqClient, btClient, btTables, metadata)

	// Subscribe to cache update
	if !*bigqueryOnly {
		err = s.SubscribeBranchCacheUpdate(
			ctx, pubsubProject, branchCacheVersionBucket, subscriberPrefix, pubsubTopic)
		if err != nil {
			log.Fatalf("Failed to subscribe to branch cache update: %v", err)
		}
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
