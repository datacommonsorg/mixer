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
	"encoding/json"
	"flag"
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
	btProject        = "google.com:datcom-store-dev"
	btInstance       = "prophet-cache"
	bqBillingProject = "datcom-ci"
)

func setup(memcache *server.Memcache) (pb.MixerClient, error) {
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	bqTableID, _ := ioutil.ReadFile(
		path.Join(path.Dir(filename), "../deployment/bigquery.txt"))
	btTableID, _ := ioutil.ReadFile(
		path.Join(path.Dir(filename), "../deployment/bigtable.txt"))

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
	metadata, err := server.NewMetadata(strings.TrimSpace(string(bqTableID)))
	if err != nil {
		return nil, err
	}

	s := server.NewServer(bqClient, btTable, memcache, metadata)
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

func loadMemcache() (map[string][]byte, error) {
	_, filename, _, _ := runtime.Caller(0)
	file, _ := ioutil.ReadFile(path.Join(path.Dir(filename), "memcache.json"))
	var memcacheTmp map[string]string
	err := json.Unmarshal(file, &memcacheTmp)
	if err != nil {
		return nil, err
	}
	memcacheData := map[string][]byte{}
	for dcid, raw := range memcacheTmp {
		memcacheData[dcid] = []byte(raw)
	}
	return memcacheData, nil
}

func updateGolden(v interface{}, fname string) {
	jsonByte, _ := json.MarshalIndent(v, "", "  ")
	err := ioutil.WriteFile(fname, jsonByte, 0644)
	if err != nil {
		log.Printf("could not write golden files to %s", fname)
	}
}

func updateProtoGolden(resp protoreflect.ProtoMessage, fname string) {
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
	err = ioutil.WriteFile(fname, jsonByte, 0644)
	if err != nil {
		log.Printf("could not write golden files to %s", fname)
	}
}
