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
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewMixerClient(conn)

	ctx := context.Background()

	// Send a bunch of Sparql query and print out its response.
	qStr := "Santa Clara"

	r, err := c.Search(ctx, &pb.SearchRequest{Query: qStr})
	if err != nil {
		log.Fatalf("could not Search: %v", err)
	}

	sections := r.GetSection()
	log.Printf("%d sections", len(sections))
	if numSection := len(sections); numSection < 5 {
		log.Fatalf("too few results from search: %d sections", numSection)
	}

	for _, section := range sections {
		if section.GetTypeName() == "County" {
			if numEntity := len(section.GetEntity()); numEntity != 1 {
				log.Fatalf("There should be one entity, found %d", numEntity)
			}
			if dcid := section.GetEntity()[0].GetDcid(); dcid != "geoId/06085" {
				log.Fatalf("Wrong dcid: %s", dcid)
			}
			if name := section.GetEntity()[0].GetName(); name != "Santa Clara County (in California)" {
				log.Fatalf("Wrong name: %s", name)
			}
		}
	}

	log.Printf("Search: %v", r.GetSection())
}
