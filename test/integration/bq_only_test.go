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

package integration

import (
	"context"
	"io/ioutil"
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestSparql(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, err := setupBqOnly()
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "golden_response/staging/query")

	for _, c := range []struct {
		sparql     string
		goldenFile string
	}{
		{
			`
			BASE <http://schema.org/>
			SELECT  ?pop ?Unemployment
			WHERE {
				?pop typeOf StatisticalPopulation .
				?o typeOf Observation .
				?pop dcid ("dc/p/qep2q2lcc3rcc" "dc/p/gmw3cn8tmsnth" "dc/p/92cxc027krdcd") .
				?o measuredProperty unemploymentRate .
				?o measurementMethod BLSSeasonallyUnadjusted .
				?o observationPeriod P1Y .
				?o observedNode ?pop .
				?o measuredValue ?Unemployment
			}
			ORDER BY DESC(?Unemployment)
			LIMIT 10`,
			"unemployment.json",
		},
	} {
		req := &pb.QueryRequest{Sparql: c.sparql}
		resp, err := client.Query(ctx, req)
		if err != nil {
			t.Errorf("could not Query: %v", err)
			continue
		}
		goldenFile := path.Join(goldenPath, c.goldenFile)
		var expected pb.QueryResponse
		file, _ := ioutil.ReadFile(goldenFile)
		if err := protojson.Unmarshal(file, &expected); err != nil {
			t.Errorf("Can not Unmarshal golden file %s: %v", c.goldenFile, err)
			continue
		}
		if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
			t.Errorf("payload got diff: %v", diff)
			continue
		}
	}
}

func TestBt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, err := setupBqOnly()
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	req := &pb.GetTriplesRequest{Dcids: []string{"geoId/06", "geoId/07"}}
	_, err = client.GetTriples(ctx, req)
	if err == nil {
		t.Errorf("Expect error for Bigtable query")
	}
	if err.Error() != "rpc error: code = NotFound desc = Bigtable instance is not specified" {
		t.Errorf("Error msg is not expected: %s", err)
	}
}
