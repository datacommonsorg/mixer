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

func TestCurry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, err := setup()
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "golden_response/staging/curry")

	for _, c := range []struct {
		sparql     string
		goldenFile string
	}{
		{
			`SELECT ?place ?date ?value
			WHERE {
				?o typeOf Observation .
				?pop typeOf StatisticalPopulation .
				?place typeOf Place .
				?o measurementMethod "GoogleKGHumanCurated" .
				?o measuredProperty count .
				?o measuredValue ?value .
				?o observationDate ?date .
				?o observedNode ?pop .
				?pop location ?place .
			}
			ORDER BY ASC (?place)`,
			"pop.json",
		},
		{
			`SELECT ?place ?date ?value
			WHERE {
				?o typeOf Observation .
				?place typeOf Place .
				?o measurementMethod "GoogleKGHumanCurated" .
				?o measuredProperty unemploymentRate .
				?o measuredValue ?value .
				?o observationDate ?date .
				?o observedNode ?place .
			}
			ORDER BY ASC (?place)`,
			"ur.json",
		},
		{
			`SELECT ?place ?date ?value
			WHERE {
				?o typeOf Observation .
				?place typeOf Place .
				?o measurementMethod "GoogleKGHumanCurated" .
				?o measuredProperty minimumWage .
				?o measuredValue ?value .
				?o observationDate ?date .
				?o observedNode ?place .
			}
			ORDER BY ASC (?place)`,
			"mw.json",
		},
	} {
		req := &pb.QueryRequest{Sparql: c.sparql}
		resp, err := client.Query(ctx, req)
		if err != nil {
			t.Errorf("could not Query: %v", err)
			continue
		}

		goldenFile := path.Join(goldenPath, c.goldenFile)
		if generateGolden {
			updateGolden(resp, goldenFile)
			continue
		}

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
