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
	"fmt"
	"log"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbs "github.com/datacommonsorg/mixer/internal/proto/service"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	addr = flag.String("addr", "127.0.0.1:12345", "Address of grpc server.")
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	flag.Parse()

	// Set up a connection to the server.
	conn, err := grpc.NewClient(*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100000000 /* 100M */)),
	)
	if err != nil {
		log.Fatalf("Could not create client: %v", err)
	}
	//nolint:errcheck // TODO: Fix pre-existing issue and remove comment.
	defer conn.Close()
	c := pbs.NewMixerClient(conn)
	ctx := context.Background()

	{
		// Get Version
		req := &pb.GetVersionRequest{}
		r, err := c.GetVersion(ctx, req)
		if err != nil {
			log.Fatalf("could not GetVersion: %s", err)
		}
		fmt.Printf("%s\n\n", r)
	}

	{
		// Get outgoing triples
		req := &pbv2.NodeRequest{
			Nodes:    []string{"Count_Person"},
			Property: "->*",
		}
		r, err := c.V2Node(ctx, req)
		if err != nil {
			log.Fatalf("could not run V2Node: %s", err)
		}
		fmt.Printf("%s\n\n", r)
	}

	{
		// Get custom observations (need --use_sqlite=true)
		req := &pbv2.ObservationRequest{
			Select:   []string{"variable", "entity", "date", "value"},
			Variable: &pbv2.DcidOrExpression{Dcids: []string{"test_var_1"}},
			Entity:   &pbv2.DcidOrExpression{Dcids: []string{"geoId/06"}},
		}
		r, err := c.V2Observation(ctx, req)
		if err != nil {
			log.Fatalf("could not run V2Observation: %s", err)
		}
		fmt.Printf("%v\n\n", r)
	}

	{
		// Get observation facets
		req := &pbv2.ObservationRequest{
			Select: []string{"variable", "entity", "facet"},
			Variable: &pbv2.DcidOrExpression{
				Dcids: []string{"CumulativeCount_MedicalConditionIncident_COVID_19_PatientDeceased"},
			},
			Entity: &pbv2.DcidOrExpression{
				Dcids: []string{"geoId/12"},
			},
		}
		r, err := c.V2Observation(ctx, req)
		if err != nil {
			log.Fatalf("could not run V2Observation: %s", err)
		}
		fmt.Printf("%s\n\n", r)
	}

	{
		// Get StatVarGroup ancestors
		req := &pbv2.NodeRequest{
			Nodes:    []string{"dc/g/Water_UsedFor-Irrigation"},
			Property: "->specializationOf+{typeOf:StatVarGroup}",
		}
		r, err := c.V2Node(ctx, req)
		if err != nil {
			log.Fatalf("could not run V2Node: %s", err)
		}
		fmt.Printf("%v\n\n", r)
	}

}
