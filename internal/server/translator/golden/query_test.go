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

package golden

import (
	"context"
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestQuery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, err := test.Setup()
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "query")

	for _, c := range []struct {
		sparql     string
		goldenFile string
	}{
		{
			`SELECT ?a WHERE {?a typeOf RaceCodeEnum} ORDER BY ASC(?a)`,
			"race_code_enum.json",
		},
		{
			`SELECT ?name
			WHERE{
				?state typeOf State .
				?state dcid geoId/06 .
				?state name ?name
			}`,
			"name.json",
		},
		{
			`SELECT ?place ?value
			WHERE {
			 ?observation typeOf StatVarObservation .
			 ?observation variableMeasured Amount_EconomicActivity_GrossNationalIncome_PurchasingPowerParity_PerCapita .
			 ?observation observationAbout ?place .
			 ?observation observationDate "2000" .
			 ?observation value ?value .
			 ?place typeOf Country .
			}
			ORDER BY ASC (?place)
			LIMIT 10`,
			"statvar-obs.json",
		},
	} {
		req := &pb.QueryRequest{Sparql: c.sparql}
		resp, err := client.Query(ctx, req)
		if err != nil {
			t.Errorf("could not Query: %v", err)
			continue
		}

		if test.GenerateGolden {
			test.UpdateGolden(resp, goldenPath, c.goldenFile)
			continue
		}

		var expected pb.QueryResponse
		if err = test.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
			t.Errorf("Can not Unmarshal golden file %s: %v", c.goldenFile, err)
			continue
		}
		if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
			t.Errorf("payload got diff: %v", diff)
			continue
		}
	}
}
