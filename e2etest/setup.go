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

package e2etest

import (
	"context"
	"io/ioutil"
	"log"
	"net"
	"path"
	"runtime"
	"strings"

	"cloud.google.com/go/bigquery"
	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// This test runs agains staging staging bt and bq dataset.
// This is billed to GCP project "datcom-ci"
// It needs Application Default Credentials to run locally or need to
// provide service account credential when running on GCP.
const (
	btProject        = "google.com:datcom-store-dev"
	btInstance       = "prophet-cache"
	bqBillingProject = "datcom-ci"
)

func setup() (pb.MixerClient, error) {
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	bqTableID, _ := ioutil.ReadFile(
		path.Join(path.Dir(filename), "../deployment/staging_bq_table.txt"))
	btTableID, _ := ioutil.ReadFile(
		path.Join(path.Dir(filename), "../deployment/staging_bt_table.txt"))

	// BigQuery.
	bqClient, err := bigquery.NewClient(ctx, bqBillingProject)
	if err != nil {
		log.Fatalf("failed to create Bigquery client: %v", err)
	}

	btTable, err := server.NewBtTable(
		ctx, btProject, btInstance, strings.TrimSpace(string(btTableID)))
	if err != nil {
		return nil, err
	}
	// TODO(boxu): Change mapping not to have dataset name in it.
	// Metadata.
	metadata, err := server.NewMetadata(path.Join(path.Dir(filename), "mapping"))
	if err != nil {
		return nil, err
	}

	memcache := server.NewMemcache(map[string][]byte{})
	s := server.NewServer(
		bqClient, btTable, memcache, metadata, strings.TrimSpace(string(bqTableID)))
	srv := grpc.NewServer()
	pb.RegisterMixerServer(srv, s)
	reflection.Register(srv)
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, err
	}
	// Start mixer at localhost:0
	go func() {
		err := srv.Serve(lis)
		if err != nil {
			log.Fatalf("failed to start mixer in localhost:0")
		}
	}()

	// Create mixer client
	conn, err := grpc.Dial(
		lis.Addr().String(),
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100000000 /* 100M */)))
	if err != nil {
		return nil, err
	}
	client := pb.NewMixerClient(conn)
	return client, nil
}
