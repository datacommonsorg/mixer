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
	"io/ioutil"
	"path"
	"runtime"
	"strings"
	"testing"

	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/server"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
)

// This test runs agains staging staging bt and bq dataset.
// This is billed to GCP project "datcom-ci"
// It needs Application Default Credentials to run locally or need to
// provide service account credential when running on GCP.
const (
	btProject   = "google.com:datcom-store-dev"
	btInstance  = "prophet-cache"
	bqProject   = "datcom-ci"
	branchCache = false
)

func Setup(ctx context.Context) (pb.MixerClient, error) {
	_, filename, _, _ := runtime.Caller(0)
	btTable, _ := ioutil.ReadFile(
		path.Join(path.Dir(filename), "../deployment/staging_bt_table.txt"))
	bqTable, _ := ioutil.ReadFile(
		path.Join(path.Dir(filename), "../deployment/staging_bq_table.txt"))
	// Use mapping template before we need to test SPARQL query.
	schemaPath := path.Join(path.Dir(filename), "../deployment/mapping")

	s, err := server.NewServer(
		"localhost:0",
		strings.TrimSpace(string(bqTable)),
		strings.TrimSpace(string(btTable)),
		btProject,
		btInstance,
		bqProject,
		schemaPath,
		branchCache,
	)
	if err != nil {
		return nil, err
	}
	// Start mixer at localhost:0
	go s.Srv.Serve(s.Lis)

	// Create mixer client
	conn, err := grpc.Dial(
		s.Addr,
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100000000 /* 100M */)))
	if err != nil {
		return nil, err
	}
	client := pb.NewMixerClient(conn)
	return client, nil
}

func TestGetStats(t *testing.T) {
	ctx := context.Background()
	client, err := Setup(ctx)
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "../golden_response/staging/get_stats")

	for _, c := range []struct {
		statsVar     string
		place        []string
		goldenFile   string
		partialMatch bool
	}{
		{
			"TotalPopulation",
			[]string{"country/USA", "geoId/06", "geoId/06085", "geoId/0649670"},
			"TotalPopulation.json",
			false,
		},
		{
			"NYTCovid19CumulativeCases",
			[]string{"country/USA", "geoId/06", "geoId/06085"},
			"NYTCovid19CumulativeCases.json",
			true,
		},
		{
			"TotalCrimes",
			[]string{"geoId/06", "geoId/0649670"},
			"TotalCrimes.json",
			false,
		},
	} {

		resp, err := client.GetStats(ctx, &pb.GetStatsRequest{
			StatsVar: c.statsVar,
			Place:    c.place,
		})
		if err != nil {
			t.Errorf("could not GetStats: %s", err)
			continue
		}
		var result map[string]*pb.ObsTimeSeries
		err = json.Unmarshal([]byte(resp.GetPayload()), &result)
		if err != nil {
			t.Errorf("Can not Unmarshal payload")
			continue
		}
		var expected map[string]*pb.ObsTimeSeries
		file, _ := ioutil.ReadFile(path.Join(goldenPath, c.goldenFile))
		err = json.Unmarshal(file, &expected)
		if err != nil {
			t.Errorf("Can not Unmarshal golden file")
			continue
		}
		if c.partialMatch {
			for geo := range expected {
				for date := range expected[geo].Data {
					if expected[geo].Data[date] != result[geo].Data[date] {
						t.Errorf("%s, %s, %s want: %f, got: %f", c.statsVar, geo,
							date, expected[geo].Data[date], result[geo].Data[date],
						)
						continue
					}
				}
			}
		} else {
			if diff := cmp.Diff(result, expected); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}

	}
}
