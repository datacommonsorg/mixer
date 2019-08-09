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
	"context"
	"flag"
	"log"
	"time"

	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/util"
	"google.golang.org/grpc"
)

var (
	addr = flag.String("addr", "127.0.0.1:12345", "Address of grpc server.")
)

func main() {
	flag.Parse()

	// Set up a connection to the server.
	conn, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewMixerClient(conn)
	ctx := context.Background()

	req := &pb.GetPlaceKMLRequest{Dcid: "geoId/04"}
	log.Printf("Requesting { %s}\n", req)

	start := time.Now()
	res, err := c.GetPlaceKML(ctx, req)
	elapsed := time.Since(start)
	if err != nil {
		log.Fatalf("GetPlaceKML() = %v", err)
	}

	jsonRaw, err := util.UnzipAndDecode(res.GetPayload())
	if err != nil {
		log.Fatalf("util.UnzipAndDecode() = %v", err)
	}
	log.Printf("%s", string(jsonRaw))

	log.Printf("Request took: %s\n\n", elapsed)
}
