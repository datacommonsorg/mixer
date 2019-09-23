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

	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/util"
	"google.golang.org/grpc"
)

var (
	addr = flag.String("addr", "127.0.0.1:12345", "Address of grpc server.")
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	flag.Parse()

	// Set up a connection to the server.
	conn, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewMixerClient(conn)

	ctx := context.Background()

	// Get PopObs for Mountain View
	getPopObs(ctx, c, "geoId/0649670")

	// Get PopObs for California
	getPopObs(ctx, c, "geoId/06")

	// No PopObs for Class
	getPopObs(ctx, c, "Class")

}

func getPopObs(ctx context.Context, c pb.MixerClient, dcid string) {
	r, err := c.GetPopObs(ctx, &pb.GetPopObsRequest{
		Dcid: dcid,
	})
	if err != nil {
		log.Fatalf("could not GetPopObs: %s", err)
	}

	log.Printf("Now printing pop obs for dcid = %s", dcid)

	jsonRaw, err := util.UnzipAndDecode(r.GetPayload())
	if err != nil {
		log.Fatalf("util.UnzipAndDecode() = %v", err)
	}

	log.Printf("%s", string(jsonRaw))
}
