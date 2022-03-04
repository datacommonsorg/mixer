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

package e2e

import (
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/protojson"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
)

// TestOption holds the options for integration test.
type TestOption struct {
	UseCache       bool
	UseMemdb       bool
	UseImportGroup bool
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
	baseInstance     = "prophet-cache"
	bqBillingProject = "datcom-ci"
	storeProject     = "datcom-store"
	tmcfCsvBucket    = "datcom-public"
	tmcfCsvPrefix    = "test"
	branchInstance   = "prophet-branch-cache"
)

// Setup creates local server and client.
func Setup(option ...*TestOption) (pb.MixerClient, pb.ReconClient, error) {
	useCache, useMemdb, useImportGroup := false, false, false
	if len(option) == 1 {
		useCache = option[0].UseCache
		useMemdb = option[0].UseMemdb
		useImportGroup = option[0].UseImportGroup
	}
	return setupInternal(
		"../../deploy/storage/bigquery.version",
		"../../deploy/storage/bigtable.version",
		"../../deploy/storage/bigtable_import_groups.version",
		"../../deploy/mapping",
		storeProject,
		useCache,
		useMemdb,
		useImportGroup,
	)
}

func setupInternal(
	bq, bt, btGroup, mcfPath, storeProject string, useCache, useMemdb, useImportGroup bool,
) (
	pb.MixerClient, pb.ReconClient, error,
) {
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	bqTableID, _ := ioutil.ReadFile(path.Join(path.Dir(filename), bq))
	baseTableName, _ := ioutil.ReadFile(path.Join(path.Dir(filename), bt))
	schemaPath := path.Join(path.Dir(filename), mcfPath)

	btGroupString, _ := ioutil.ReadFile(path.Join(path.Dir(filename), btGroup))
	tableNames := strings.Split(string(btGroupString), "\n")

	// BigQuery.
	bqClient, err := bigquery.NewClient(ctx, bqBillingProject)
	if err != nil {
		log.Fatalf("failed to create Bigquery client: %v", err)
	}

	var branchTableName string
	if useImportGroup {
		branchTableName = "dcbranch_2022_03_01_18_18_35"
	} else {
		branchTableName = "branch_dcbranch_2022_03_01_16_16_50"
	}

	tables := []*bigtable.Table{}
	branchTable, err := bigtable.NewBtTable(ctx, storeProject, branchInstance, branchTableName)
	if err != nil {
		return nil, nil, err
	}
	tables = append(tables, bigtable.NewTable(branchTableName, branchTable))

	if useImportGroup {
		for _, t := range tableNames {
			name := strings.TrimSpace(t)
			table, err := bigtable.NewBtTable(ctx, storeProject, baseInstance, name)
			if err != nil {
				return nil, nil, err
			}
			tables = append(tables, bigtable.NewTable(name, table))
		}
	} else {
		name := strings.TrimSpace(string(baseTableName))
		baseTable, err := bigtable.NewBtTable(ctx, storeProject, baseInstance, name)
		if err != nil {
			return nil, nil, err
		}
		tables = append(tables, bigtable.NewTable(name, baseTable))
	}

	metadata, err := server.NewMetadata(
		strings.TrimSpace(string(bqTableID)), storeProject, "", schemaPath)
	if err != nil {
		return nil, nil, err
	}
	var cache *resource.Cache
	if useCache {
		cache, err = server.NewCache(
			ctx, store.NewStore(nil, nil, tables, branchTableName, useImportGroup))
		if err != nil {
			return nil, nil, err
		}
	} else {
		cache = &resource.Cache{}
	}
	memDb := memdb.NewMemDb()
	if useMemdb {
		err = memDb.LoadFromGcs(ctx, tmcfCsvBucket, tmcfCsvPrefix)
		if err != nil {
			log.Fatalf("Failed to load tmcf and csv from GCS: %v", err)
		}
	}
	return newClient(bqClient, tables, metadata, cache, memDb, useImportGroup, branchTableName)
}

// SetupBqOnly creates local server and client with access to BigQuery only.
func SetupBqOnly() (pb.MixerClient, pb.ReconClient, error) {
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	bqTableID, _ := ioutil.ReadFile(
		path.Join(path.Dir(filename), "../../deploy/storage/bigquery.version"))
	schemaPath := path.Join(path.Dir(filename), "../../deploy/mapping/")

	// BigQuery.
	bqClient, err := bigquery.NewClient(ctx, bqBillingProject)
	if err != nil {
		log.Fatalf("failed to create Bigquery client: %v", err)
	}
	metadata, err := server.NewMetadata(
		strings.TrimSpace(string(bqTableID)),
		storeProject,
		"",
		schemaPath)
	if err != nil {
		return nil, nil, err
	}
	return newClient(bqClient, nil, metadata, nil, nil, false, "")
}

func newClient(
	bqClient *bigquery.Client,
	tables []*bigtable.Table,
	metadata *resource.Metadata,
	cache *resource.Cache,
	memDb *memdb.MemDb,
	useImportGroup bool,
	branchTableName string,
) (pb.MixerClient, pb.ReconClient, error) {
	mixerStore := store.NewStore(bqClient, memDb, tables, branchTableName, useImportGroup)
	reconStore := store.NewStore(nil, nil, tables, "", useImportGroup)
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
		grpc.WithInsecure(),
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
	jsonByte, _ := json.MarshalIndent(v, "", "  ")
	if ioutil.WriteFile(path.Join(root, fname), jsonByte, 0644) != nil {
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
		log.Printf("could not write golden files to %s", fname)
		return
	}
	var rm json.RawMessage = data
	jsonByte, err := json.MarshalIndent(rm, "", "  ")
	if err != nil {
		log.Printf("could not write golden files to %s", fname)
		return
	}
	err = ioutil.WriteFile(path.Join(root, fname), jsonByte, 0644)
	if err != nil {
		log.Printf("could not write golden files to %s", fname)
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
