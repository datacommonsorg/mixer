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
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestQuery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, err := setup()
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "golden_response/query")

	for _, c := range []struct {
		sparql     string
		goldenFile string
	}{
		{
			`BASE <http://schema.org/>
			SELECT ?MeanTemp
			WHERE {
				?o typeOf DailyWeatherObservation .
				?o measuredProperty temperature .
				?o meanValue ?MeanTemp .
				?o observationDate "2019-01-01" .
				?o observedNode ?place .
				?place dcid geoId/4261000
			}
			LIMIT 10`,
			"weather1.json",
		},
		{
			`
			SELECT ?date ?mean ?unit
			WHERE {
				?o typeOf MonthlyWeatherObservation .
				?o observedNode geoId/0649670 .
				?o measuredProperty temperature .
				?o observationDate ?date .
				?o unit ?unit .
				?o meanValue ?mean .
			}
			ORDER BY ASC(?date)
			`,
			"weather2.json",
		},
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

		if generateGolden {
			updateGolden(resp, goldenPath, c.goldenFile)
			continue
		}

		var expected pb.QueryResponse
		if err = readJSON(goldenPath, c.goldenFile, &expected); err != nil {
			t.Errorf("Can not Unmarshal golden file %s: %v", c.goldenFile, err)
			continue
		}
		if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
			t.Errorf("payload got diff: %v", diff)
			continue
		}
	}
}
