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
	"encoding/json"
	"io/ioutil"
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/pkg/proto"
	"github.com/datacommonsorg/mixer/pkg/server"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetObservations(t *testing.T) {
	ctx := context.Background()
	client, err := setup(server.NewMemcache(map[string][]byte{}))
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "golden_response/staging/get_observations")

	for _, c := range []struct {
		dcids      []string
		mprop      string
		statsType  string
		obsDate    string
		obsPeriod  string
		mmethod    string
		goldenFile string
	}{
		{
			[]string{"dc/p/x6t44d8jd95rd", "dc/p/lr52m1yr46r44"},
			"count",
			"measuredValue",
			"2018-12",
			"P1M",
			"BLSSeasonallyAdjusted",
			"employment.json",
		},
		{
			[]string{"dc/p/2ygbv16ky4yvb", "dc/p/cg941cc1lbsvb"},
			"count",
			"measuredValue",
			"2015",
			"",
			"CensusACS5yrSurvey",
			"total_count.json",
		},
	} {
		req := &pb.GetObservationsRequest{
			Dcids:             c.dcids,
			MeasuredProperty:  c.mprop,
			StatsType:         c.statsType,
			ObservationDate:   c.obsDate,
			ObservationPeriod: c.obsPeriod,
			MeasurementMethod: c.mmethod,
		}
		resp, err := client.GetObservations(ctx, req)
		if err != nil {
			t.Errorf("could not GetObservations: %s", err)
			continue
		}
		var result []*server.PopObs
		err = json.Unmarshal([]byte(resp.GetPayload()), &result)
		if err != nil {
			t.Errorf("Can not Unmarshal payload")
			continue
		}
		var expected []*server.PopObs
		file, _ := ioutil.ReadFile(path.Join(goldenPath, c.goldenFile))
		err = json.Unmarshal(file, &expected)
		if err != nil {
			t.Errorf("Can not Unmarshal golden file %s: %v", c.goldenFile, err)
			continue
		}
		if diff := cmp.Diff(result, expected, protocmp.Transform()); diff != "" {
			t.Errorf("payload got diff: %v", diff)
			continue
		}
	}
}
