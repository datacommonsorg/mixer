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

package test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	_ "github.com/mattn/go-sqlite3" // import the sqlite3 driver

	"cloud.google.com/go/bigquery"
	pbs "github.com/datacommonsorg/mixer/internal/proto/service"
	"github.com/datacommonsorg/mixer/internal/server"
	"github.com/datacommonsorg/mixer/internal/server/cache"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/protojson"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	"googlemaps.github.io/maps"
)

// TestOption holds the options for integration test.
type TestOption struct {
	FetchSVG          bool
	SearchSVG         bool
	UseCustomTable    bool
	UseSQLite         bool
	RemoteMixerDomain string
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
	bigqueryBillingProject = "datcom-store"
	hostProject            = "datcom-ci"
)

// Setup creates local server and client.
func Setup(option ...*TestOption) (pbs.MixerClient, error) {
	fetchSVG, searchSVG, useCustomTable, useSQLite, remoteMixerDomain := false, false, false, false, ""
	var cacheOptions cache.Options
	if len(option) == 1 {
		fetchSVG = option[0].FetchSVG
		searchSVG = option[0].SearchSVG
		useCustomTable = option[0].UseCustomTable
		useSQLite = option[0].UseSQLite
		cacheOptions.CustomProv = useSQLite
		cacheOptions.FetchSVG = fetchSVG
		cacheOptions.SearchSVG = searchSVG
		remoteMixerDomain = option[0].RemoteMixerDomain
	}
	return setupInternal(
		"../deploy/storage/bigquery.version",
		"../deploy/storage/base_bigtable_info.yaml",
		"./custom_bigtable_info.yaml",
		"../deploy/mapping",
		useCustomTable,
		useSQLite,
		cacheOptions,
		remoteMixerDomain,
	)
}

func setupInternal(
	bigqueryVersionFile, baseBigtableInfoYaml, testBigtableInfoYaml, mcfPath string,
	useCustomTable, useSQLite bool,
	cacheOptions cache.Options,
	remoteMixerDomain string,
) (pbs.MixerClient, error) {
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	bqTableID, _ := os.ReadFile(path.Join(path.Dir(filename), bigqueryVersionFile))
	schemaPath := path.Join(path.Dir(filename), mcfPath)

	baseBigtableInfo, _ := os.ReadFile(path.Join(path.Dir(filename), baseBigtableInfoYaml))
	tables, err := bigtable.CreateBigtables(ctx, string(baseBigtableInfo), false /*isCustom=*/)
	if err != nil {
		log.Fatalf("failed to create Bigtable tables: %v", err)
	}
	if useCustomTable {
		customBigtableInfo, _ := os.ReadFile(path.Join(path.Dir(filename), testBigtableInfoYaml))
		customTables, err := bigtable.CreateBigtables(ctx, string(customBigtableInfo), true /*isCustom=*/)
		if err != nil {
			log.Fatalf("failed to create Bigtable tables: %v", err)
		}
		tables = append(tables, customTables...)
	}
	// BigQuery.
	bqClient, err := bigquery.NewClient(ctx, bigqueryBillingProject)
	if err != nil {
		log.Fatalf("failed to create Bigquery client: %v", err)
	}
	// SQLite
	var sqlClient *sql.DB
	if useSQLite {
		sqlClient, err = sql.Open("sqlite3", filepath.Join(path.Dir(filename), "./datacommons.db"))
		if err != nil {
			log.Fatalf("Failed to read sqlite3 database: %v", err)
		}
	}

	metadata, err := server.NewMetadata(
		ctx,
		hostProject,
		strings.TrimSpace(string(bqTableID)),
		schemaPath,
		remoteMixerDomain,
		false,
	)
	if err != nil {
		return nil, err
	}
	st, err := store.NewStore(bqClient, sqlClient, tables, "", metadata)
	if err != nil {
		log.Fatalf("Failed to create a new store: %s", err)
	}
	c, err := cache.NewCache(ctx, st, cacheOptions)
	if err != nil {
		return nil, err
	}
	mapsClient, err := util.MapsClient(ctx, metadata.HostProject)
	if err != nil {
		return nil, err
	}

	return newClient(st, tables, metadata, c, mapsClient)
}

// SetupBqOnly creates local server and client with access to BigQuery only.
func SetupBqOnly() (pbs.MixerClient, error) {
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	bqTableID, _ := os.ReadFile(
		path.Join(path.Dir(filename), "../deploy/storage/bigquery.version"))
	schemaPath := path.Join(path.Dir(filename), "../deploy/mapping/")

	// BigQuery.
	bqClient, err := bigquery.NewClient(ctx, bigqueryBillingProject)
	if err != nil {
		log.Fatalf("failed to create Bigquery client: %v", err)
	}
	metadata, err := server.NewMetadata(
		ctx,
		"",
		strings.TrimSpace(string(bqTableID)),
		schemaPath,
		"",
		false,
	)
	if err != nil {
		return nil, err
	}
	st, err := store.NewStore(bqClient, nil, nil, "", nil)
	if err != nil {
		return nil, err
	}
	return newClient(st, nil, metadata, nil, nil)
}

func newClient(
	mixerStore *store.Store,
	tables []*bigtable.Table,
	metadata *resource.Metadata,
	cachedata *cache.Cache,
	mapsClient *maps.Client,
) (pbs.MixerClient, error) {
	mixerServer := server.NewMixerServer(mixerStore, metadata, cachedata, mapsClient)
	srv := grpc.NewServer()
	pbs.RegisterMixerServer(srv, mixerServer)
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
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(300000000 /* 300M */)))
	if err != nil {
		return nil, err
	}
	mixerClient := pbs.NewMixerClient(conn)
	return mixerClient, nil
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
	if os.WriteFile(
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
	err = os.WriteFile(path.Join(root, fname), jsonByte, 0644)
	if err != nil {
		log.Printf("os.WriteFile(%s) = %s", fname, err)
	}
}

// ReadJSON reads in the golden Json file.
func ReadJSON(dir, fname string, resp protoreflect.ProtoMessage) error {
	bytes, err := os.ReadFile(path.Join(dir, fname))
	if err != nil {
		return err
	}
	err = protojson.Unmarshal(bytes, resp)
	if err != nil {
		return err
	}
	return nil
}
