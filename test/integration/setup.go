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

package integration

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"net"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/protojson"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
)

var generateGolden bool

func init() {
	flag.BoolVar(
		&generateGolden, "generate_golden", false, "generate golden files")
}

// This test runs agains staging staging bt and bq dataset.
// This is billed to GCP project "datcom-ci"
// It needs Application Default Credentials to run locally or need to
// provide service account credential when running on GCP.
const (
	baseInstance     = "prophet-cache"
	bqBillingProject = "datcom-ci"
	store            = "datcom-store"
	lineLimit        = 10000
)

func setup() (pb.MixerClient, error) {
	return setupInternal(
		"../../deploy/storage/bigquery.version",
		"../../deploy/storage/bigtable.version",
		"../../deploy/mapping",
		store)
}

func setupInternal(
	bq, bt, mcfPath, storeProject string) (pb.MixerClient, error) {
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	bqTableID, _ := ioutil.ReadFile(path.Join(path.Dir(filename), bq))
	baseTableName, _ := ioutil.ReadFile(path.Join(path.Dir(filename), bt))
	schemaPath := path.Join(path.Dir(filename), mcfPath)

	// BigQuery.
	bqClient, err := bigquery.NewClient(ctx, bqBillingProject)
	if err != nil {
		log.Fatalf("failed to create Bigquery client: %v", err)
	}

	baseTable, err := server.NewBtTable(
		ctx, storeProject, baseInstance, strings.TrimSpace(string(baseTableName)))
	if err != nil {
		return nil, err
	}

	branchTable, err := createBranchTable(ctx)
	if err != nil {
		return nil, err
	}

	metadata, err := server.NewMetadata(
		strings.TrimSpace(string(bqTableID)), storeProject, "", schemaPath)
	if err != nil {
		return nil, err
	}

	cache, err := server.NewCache(ctx, baseTable)
	if err != nil {
		return nil, err
	}
	return newClient(bqClient, baseTable, branchTable, metadata, cache)
}

func setupBqOnly() (pb.MixerClient, error) {
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
		store,
		"",
		schemaPath)
	if err != nil {
		return nil, err
	}
	return newClient(bqClient, nil, nil, metadata, nil)
}

func newClient(
	bqClient *bigquery.Client,
	baseTable *bigtable.Table,
	branchTable *bigtable.Table,
	metadata *server.Metadata,
	cache *server.Cache) (pb.MixerClient, error) {
	s := server.NewServer(bqClient, baseTable, branchTable, metadata, cache)
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

func createBranchTable(ctx context.Context) (*bigtable.Table, error) {
	_, filename, _, _ := runtime.Caller(0)
	file, _ := ioutil.ReadFile(path.Join(path.Dir(filename), "memcache.json"))
	var data map[string]string
	err := json.Unmarshal(file, &data)
	if err != nil {
		return nil, err
	}
	return server.SetupBigtable(ctx, data)
}

func updateGolden(v interface{}, fname string, shard ...bool) {
	jsonByte, _ := json.MarshalIndent(v, "", "  ")
	var err error
	if len(shard) == 1 && shard[0] {
		err = writeJsonShard(jsonByte, fname)
	} else {
		err = ioutil.WriteFile(fname, jsonByte, 0644)
	}
	if err != nil {
		log.Printf("could not write golden files to %s", fname)
	}
}

func updateProtoGolden(
	resp protoreflect.ProtoMessage, fname string, shard ...bool) {
	var err error
	marshaller := protojson.MarshalOptions{Indent: ""}
	// protojson don't and won't make stable output: https://github.com/golang/protobuf/issues/1082
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
	if len(shard) == 1 && shard[0] {
		err = writeJsonShard(jsonByte, fname)
	} else {
		err = ioutil.WriteFile(fname, jsonByte, 0644)
	}
	if err != nil {
		log.Printf("could not write golden files to %s", fname)
	}
}

func writeJsonShard(jsonByte []byte, fname string) error {
	jsonLines := strings.Split(string(jsonByte), "\n")
	for i := 0; ; i++ {
		start := i * lineLimit
		end := (i + 1) * lineLimit
		if end > len(jsonLines) {
			end = len(jsonLines)
		}
		err := ioutil.WriteFile(
			fmt.Sprintf("%s.%03d", fname, i),
			[]byte(strings.Join(jsonLines[start:end], "\n")),
			0644,
		)
		if err != nil {
			return err
		}
		if end == len(jsonLines) {
			break
		}
	}
	return nil
}

func find(path, fname string) ([]string, error) {
	var result []string
	// WalkDir outputs is lexically sorted
	err := filepath.WalkDir(path, func(s string, d fs.DirEntry, e error) error { //nolint
		if e != nil {
			return e
		}
		if strings.Contains(s, fname) {
			result = append(result, s)
		}
		return nil
	})
	return result, err
}

func readJsonShard(path, fname string) ([]byte, error) {
	fileNames, err := find(path, fname)
	if err != nil {
		return nil, err
	}
	allBytes := []byte{}
	for _, name := range fileNames {
		bytes, err := ioutil.ReadFile(name)
		if err != nil {
			return nil, err
		}
		allBytes = append(allBytes, bytes...)
	}
	return allBytes, nil
}
