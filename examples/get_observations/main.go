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

	// Get Observation for count
	getObservation(ctx, c, []string{"dc/p/2ygbv16ky4yvb", "dc/p/cg941cc1lbsvb"},
		"count", "2015", "measured_value", "", "CenusACS5yrSurvey")

	// Get Observation for income
	getObservation(ctx, c, []string{"dc/p/04z1y6c67r448", "dc/p/04zmmqm0fpfn1"},
		"income", "2015", "median_value", "", "CenusACS5yrSurvey")

	// Get Observation for income
	getObservation(ctx, c, []string{"dc/p/053kf3t9s9bg"},
		"count", "2012-07", "measured_value", "P1M", "")

	// Observation period and measurement method are not specified.
	getObservation(ctx, c, []string{"dc/p/dxxmsf6txdgx4"},
		"count", "2017", "measured_value", "", "")
}

func getObservation(ctx context.Context, c pb.MixerClient, dcids []string,
	measuredProperty, observationDate, statsType, observationPeriod, measurementMethod string) {
	r, err := c.GetObservations(ctx, &pb.GetObservationsRequest{
		Dcids:             dcids,
		MeasuredProperty:  measuredProperty,
		ObservationDate:   observationDate,
		StatsType:         statsType,
		ObservationPeriod: observationPeriod,
		MeasurementMethod: measurementMethod,
	})
	if err != nil {
		log.Fatalf("could not GetObservations: %s", err)
	}
	log.Printf("Now printing observations for dcid = %s", dcids)
	log.Printf("%s", r.GetPayload())
}
