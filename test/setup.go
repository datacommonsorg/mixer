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

package test

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
	"runtime"
	"strings"

	"cloud.google.com/go/bigquery"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/store/memdb"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/protojson"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
)

// TestOption holds the options for integration test.
type TestOption struct {
	UseCache      bool
	UseMemdb      bool
	SearchOptions server.SearchOptions
}

var (
	// LatencyTest is used to check whether to do latency test for import group.
	LatencyTest = os.Getenv("LATENCY_TEST") == "true"
	// GenerateGolden is used to check whether generating golden.
	GenerateGolden = os.Getenv("GENERATE_GOLDEN") == "true"
)

// This test runs agains staging staging bt and bq dataset.
// This is billed to GCP project "datcom-ci"
// It needs Application Default Credentials to run locally or need to
// provide service account credential when running on GCP.
const (
	baseInstance          = "prophet-cache"
	bqBillingProject      = "datcom-ci"
	storeProject          = "datcom-store"
	tmcfCsvBucket         = "datcom-public"
	tmcfCsvPrefix         = "food"
	customBigtableProject = "datcom-mixer-autopush"
)

// Setup creates local server and client.
func Setup(option ...*TestOption) (pb.MixerClient, pb.ReconClient, error) {
	useCache, useMemdb := false, false
	var searchOptions server.SearchOptions
	if len(option) == 1 {
		useCache = option[0].UseCache
		useMemdb = option[0].UseMemdb
		searchOptions = option[0].SearchOptions
	}
	return setupInternal(
		"../deploy/storage/bigquery.version",
		"../deploy/storage/bigtable_import_groups.version",
		"../deploy/mapping",
		storeProject,
		useCache,
		useMemdb,
		searchOptions,
	)
}

func setupInternal(
	bq, btGroup, mcfPath, storeProject string,
	useCache bool, useMemdb bool, searchOptions server.SearchOptions,
) (pb.MixerClient, pb.ReconClient, error) {
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	bqTableID, _ := ioutil.ReadFile(path.Join(path.Dir(filename), bq))
	schemaPath := path.Join(path.Dir(filename), mcfPath)

	btGroupString, _ := ioutil.ReadFile(path.Join(path.Dir(filename), btGroup))
	tableNames := util.ParseBigtableGroup(string(btGroupString))

	// BigQuery.
	bqClient, err := bigquery.NewClient(ctx, bqBillingProject)
	if err != nil {
		log.Fatalf("failed to create Bigquery client: %v", err)
	}

	tables := []*bigtable.Table{}

	for _, name := range tableNames {
		table, err := bigtable.NewBtTable(ctx, storeProject, baseInstance, name)
		if err != nil {
			return nil, nil, err
		}
		tables = append(tables, bigtable.NewTable(name, table))
	}

	metadata, err := server.NewMetadata(customBigtableProject,
		strings.TrimSpace(string(bqTableID)), storeProject, "", schemaPath)
	if err != nil {
		return nil, nil, err
	}
	memDb := memdb.NewMemDb()
	if useMemdb {
		err = memDb.LoadConfig(ctx, path.Join(path.Dir(filename), "memdb.json"))
		if err != nil {
			log.Fatalf("Failed to load config: %v", err)
		}
		err = memDb.LoadFromGcs(ctx, tmcfCsvBucket, tmcfCsvPrefix)
		if err != nil {
			log.Fatalf("Failed to load tmcf and csv from GCS: %v", err)
		}
	}
	st := store.NewStore(bqClient, memDb, tables, "", metadata)
	var cache *resource.Cache
	if useCache {
		cache, err = server.NewCache(ctx, st, searchOptions)
		if err != nil {
			return nil, nil, err
		}
	} else {
		cache = &resource.Cache{}
	}
	return newClient(st, tables, metadata, cache)
}

// SetupBqOnly creates local server and client with access to BigQuery only.
func SetupBqOnly() (pb.MixerClient, pb.ReconClient, error) {
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	bqTableID, _ := ioutil.ReadFile(
		path.Join(path.Dir(filename), "../deploy/storage/bigquery.version"))
	schemaPath := path.Join(path.Dir(filename), "../deploy/mapping/")

	// BigQuery.
	bqClient, err := bigquery.NewClient(ctx, bqBillingProject)
	if err != nil {
		log.Fatalf("failed to create Bigquery client: %v", err)
	}
	metadata, err := server.NewMetadata(
		"",
		strings.TrimSpace(string(bqTableID)),
		storeProject,
		"",
		schemaPath)
	if err != nil {
		return nil, nil, err
	}
	st := store.NewStore(bqClient, nil, nil, "", nil)
	return newClient(st, nil, metadata, nil)
}

func newClient(
	mixerStore *store.Store,
	tables []*bigtable.Table,
	metadata *resource.Metadata,
	cache *resource.Cache,
) (pb.MixerClient, pb.ReconClient, error) {
	reconStore := store.NewStore(nil, nil, tables, "", metadata)
	mixerServer := server.NewMixerServer(mixerStore, metadata, cache)
	reconServer := server.NewReconServer(reconStore)
	srv := grpc.NewServer()
	pb.RegisterMixerServer(srv, mixerServer)
	pb.RegisterReconServer(srv, reconServer)
	reflection.Register(srv)
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, nil, err
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
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(300000000 /* 300M */)))
	if err != nil {
		return nil, nil, err
	}
	mixerClient := pb.NewMixerClient(conn)
	reconClient := pb.NewReconClient(conn)
	return mixerClient, reconClient, nil
}

// UpdateGolden updates the golden file for native typed response.
func UpdateGolden(v interface{}, root, fname string) {
	buf := &bytes.Buffer{}
	encoder := json.NewEncoder(buf)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	err := encoder.Encode(v)
	if err != nil {
		log.Printf("could not encode golden response %v", err)
	}
	if ioutil.WriteFile(
		path.Join(root, fname), bytes.TrimRight(buf.Bytes(), "\n"), 0644) != nil {
		log.Printf("could not write golden files to %s", fname)
	}
}

// UpdateProtoGolden updates the golden file for protobuf response.
func UpdateProtoGolden(
	resp protoreflect.ProtoMessage, root string, fname string) {
	var err error
	marshaller := protojson.MarshalOptions{Indent: ""}
	// protojson doesn't and won't make stable output:
	// https://github.com/golang/protobuf/issues/1082
	// Use encoding/json to get stable output.
	data, err := marshaller.Marshal(resp)
	if err != nil {
		log.Printf("marshaller.Marshal(%s) = %s", fname, err)
		return
	}
	var rm json.RawMessage = data
	jsonByte, err := json.MarshalIndent(rm, "", "  ")
	if err != nil {
		log.Printf("json.MarshalIndent(%s) = %s", fname, err)
		return
	}
	err = ioutil.WriteFile(path.Join(root, fname), jsonByte, 0644)
	if err != nil {
		log.Printf("ioutil.WriteFile(%s) = %s", fname, err)
	}
}

// ReadJSON reads in the golden Json file.
func ReadJSON(dir, fname string, resp protoreflect.ProtoMessage) error {
	bytes, err := ioutil.ReadFile(path.Join(dir, fname))
	if err != nil {
		return err
	}
	err = protojson.Unmarshal(bytes, resp)
	if err != nil {
		return err
	}
	return nil
}
