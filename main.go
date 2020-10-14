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
	"log"
	"net"

	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/server"

	"cloud.google.com/go/bigquery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	bqDataset    = flag.String("bq_dataset", "", "DataCommons BigQuery dataset.")
	btTable      = flag.String("bt_table", "", "DataCommons Bigtable table.")
	btProject    = flag.String("bt_project", "", "GCP project containing the BigTable instance.")
	btInstance   = flag.String("bt_instance", "", "BigTable instance.")
	projectID    = flag.String("project_id", "", "The cloud project to run the mixer instance.")
	branchFolder = flag.String("branch_folder", "", "The branch cache gcs folder.")
	port         = flag.String("port", ":12345", "Port on which to run the server.")
)

const (
	branchCacheBucket      = "prophet_cache"
	branchCacheVersionFile = "latest_branch_cache_version.txt"
	pubsubProject          = "google.com:datcom-store-dev"
	pubsubTopic            = "branch-cache-reload"
	subscriberPrefix       = "mixer-subscriber-"
)

func main() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	ctx := context.Background()

	// BigQuery.
	bqClient, err := bigquery.NewClient(ctx, *projectID)
	if err != nil {
		log.Fatalf("failed to create Bigquery client: %v", err)
	}
	// BigTable.
	btTable, err := server.NewBtTable(ctx, *btProject, *btInstance, *btTable)
	if err != nil {
		log.Fatalf("failed to create BigTable client: %v", err)
	}
	// Metadata.
	metadata, err := server.NewMetadata(*bqDataset)
	if err != nil {
		log.Fatalf("failed to create metadata: %v", err)
	}
	// Memcache
	branchCacheFolder := *branchFolder
	if branchCacheFolder == "" {
		branchCacheFolder, err = server.ReadBranchCacheFolder(
			ctx, branchCacheBucket, branchCacheVersionFile)
		if err != nil {
			log.Fatalf("failed to read branch cache folder: %v", err)
		}
	}

	memcache, err := server.NewMemcacheFromGCS(
		ctx, branchCacheBucket, branchCacheFolder)
	if err != nil {
		log.Fatalf("failed to create memcache from gcs: %v", err)
	}
	// Create server object
	s := server.NewServer(bqClient, btTable, memcache, metadata)
	// Subscribe to cache update
	err = s.SubscribeBranchCacheUpdate(
		ctx, pubsubProject, branchCacheBucket, subscriberPrefix, pubsubTopic)
	if err != nil {
		log.Fatalf("failed to subscribe to branch cache update: %v", err)
	}
	// Start mixer
	srv := grpc.NewServer()
	pb.RegisterMixerServer(srv, s)
	// Register reflection service on gRPC server.
	reflection.Register(srv)
	// Listen on network
	lis, err := net.Listen("tcp", *port)
	if err != nil {
		log.Fatalf("failed to listen on network: %v", err)
	}
	log.Println("mixer ready to server")
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}
