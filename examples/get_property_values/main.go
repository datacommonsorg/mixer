// Copyright 2019 Google LLC
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
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"encoding/json"

	pb "github.com/datacommonsorg/mixer/proto"
	"google.golang.org/grpc"
)

var (
	addr = flag.String("addr", "127.0.0.1:12345", "Address of grpc server.")
)

func main() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Set up a connection to the server.
	conn, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewMixerClient(conn)
	ctx := context.Background()

	// Get neighboring nodes for Santa Clara County, Montgomery County
	dcids := []string{"geoId/06085", "geoId/24031"}
	for _, req := range []*pb.GetPropertyValuesRequest{
		&pb.GetPropertyValuesRequest{
			Dcids:     dcids,
			ValueType: "Town",
			Property:  "containedInPlace",
			Limit:     3,
			InArc:     true,
		},
		&pb.GetPropertyValuesRequest{
			Dcids:    dcids,
			Property: "containedInPlace",
		},
		&pb.GetPropertyValuesRequest{
			Dcids:    dcids,
			Property: "containedInPlace",
		},
		&pb.GetPropertyValuesRequest{
			Dcids:    dcids,
			Property: "name",
		},
	} {
		// Call GetPropertyValues
		if err := testGetPropertyValue(ctx, c, req); err != nil {
			log.Printf("Error: %v", err)
		}
	}
}

func testGetPropertyValue(ctx context.Context, c pb.MixerClient, req *pb.GetPropertyValuesRequest) error {
	// Call GetPropertyValues
	fmt.Printf("Requesting { %s}\n", req)
	start := time.Now()
	res, err := c.GetPropertyValues(ctx, req)
	elapsed := time.Since(start)
	if err != nil {
		return err
	}

	// Format the payload
	jsonByte := []byte(res.GetPayload())
	var jsonFmt bytes.Buffer
	err = json.Indent(&jsonFmt, jsonByte, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(jsonFmt.Bytes()))
	fmt.Printf("Request took: %s\n\n", elapsed)
	return nil
}
