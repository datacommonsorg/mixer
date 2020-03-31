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
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	pb "github.com/datacommonsorg/mixer/proto"
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

	requests := []*pb.GetRelatedPlacesRequest{
		{
			Dcids:             []string{"geoId/06085"},
			PopulationType:    "Person",
			MeasuredProperty:  "count",
			MeasurementMethod: "CensusACS5yrSurvey",
			StatType:          "measuredValue",
		},
		{
			Dcids:          []string{"geoId/06085"},
			PopulationType: "Person",
			Pvs: []*pb.PropertyValue{
				{
					Property: "incomeStatus",
					Value:    "WithIncome",
				},
				{
					Property: "age",
					Value:    "Years15Onwards",
				},
			},
			MeasuredProperty:  "income",
			MeasurementMethod: "CensusACS5yrSurvey",
			StatType:          "medianValue",
		},
		{
			Dcids:             []string{"geoId/06085"},
			PopulationType:    "Person",
			MeasuredProperty:  "age",
			MeasurementMethod: "CensusACS5yrSurvey",
			StatType:          "medianValue",
		},
		{
			Dcids:             []string{"geoId/06085"},
			PopulationType:    "Person",
			MeasuredProperty:  "unemploymentRate",
			MeasurementMethod: "BLSSeasonallyUnadjusted",
			StatType:          "measuredValue",
		},
		{
			Dcids:          []string{"geoId/0649670"},
			PopulationType: "CriminalActivities",
			Pvs: []*pb.PropertyValue{
				{
					Property: "crimeType",
					Value:    "UCR_CombinedCrime",
				},
			},
			MeasuredProperty: "count",
			StatType:         "measuredValue",
		},
	}

	for _, r := range requests {
		fmt.Printf("Testing for related places.\n")
		if testGetRelatedPlaces(ctx, c, r); err != nil {
			log.Printf("Error: %v", err)
		}

		fmt.Printf("Testing for related places with same place type.\n")
		r.SamePlaceType = true
		if testGetRelatedPlaces(ctx, c, r); err != nil {
			log.Printf("Error: %v", err)
		}
		r.SamePlaceType = false

		fmt.Printf("Testing for related places with same ancestor.\n")
		r.WithinPlace = "geoId/06"
		if testGetRelatedPlaces(ctx, c, r); err != nil {
			log.Printf("Error: %v", err)
		}

		fmt.Printf("Testing for related places with same type and ancestor.\n")
		r.SamePlaceType = true
		if testGetRelatedPlaces(ctx, c, r); err != nil {
			log.Printf("Error: %v", err)
		}
		r.SamePlaceType = false
		r.WithinPlace = ""

		if r.MeasuredProperty == "count" {
			fmt.Printf("Testing for related places, per capita.\n")
			r.IsPerCapita = true
			if testGetRelatedPlaces(ctx, c, r); err != nil {
				log.Printf("Error: %v", err)
			}
			r.IsPerCapita = false
		}

		fmt.Printf("Testing for all places or top/bottom 1000 places.\n")
		r.Dcids = []string{"*"}
		if testGetRelatedPlaces(ctx, c, r); err != nil {
			log.Printf("Error: %v", err)
		}

		if r.MeasuredProperty == "count" {
			fmt.Printf("Testing for all places or top/bottom 1000 places, per capita.\n")
			if testGetRelatedPlaces(ctx, c, r); err != nil {
				log.Printf("Error: %v", err)
			}
		}
	}
}

func testGetRelatedPlaces(ctx context.Context, c pb.MixerClient,
	req *pb.GetRelatedPlacesRequest) error {
	fmt.Printf("Requesting { %s}\n", req)
	start := time.Now()
	res, err := c.GetRelatedPlaces(ctx, req)
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
