// Copyright 2024 Google LLC
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

	pbs "github.com/datacommonsorg/mixer/internal/proto/service"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	pbv3 "github.com/datacommonsorg/mixer/internal/proto/v3"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestV3Observation(t *testing.T) {
	// TODO: Remove check once enabled.
	if !test.EnableSpannerGraph {
		return
	}
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Dir(filename)

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			req        *pbv3.ObservationRequest
			goldenFile string
		}{
			{
				req: &pbv3.ObservationRequest{
					Variable: &pbv2.DcidOrExpression{
						Dcids: []string{"AirPollutant_Cancer_Risk"},
					},
					Entity: &pbv2.DcidOrExpression{
						Dcids: []string{"geoId/01001", "geoId/02013"},
					},
					Date: "",
				},
				goldenFile: "observations_all_dates.json",
			},
			{
				req: &pbv3.ObservationRequest{
					Variable: &pbv2.DcidOrExpression{
						Dcids: []string{"AirPollutant_Cancer_Risk"},
					},
					Entity: &pbv2.DcidOrExpression{
						Dcids: []string{"geoId/01001", "geoId/02013"},
					},
					Date: "LATEST",
				},
				goldenFile: "observations_latest_date.json",
			},
			{
				req: &pbv3.ObservationRequest{
					Variable: &pbv2.DcidOrExpression{
						Dcids: []string{"AirPollutant_Cancer_Risk"},
					},
					Entity: &pbv2.DcidOrExpression{
						Dcids: []string{"geoId/01001", "geoId/02013"},
					},
					Date: "2020",
				},
				goldenFile: "observations_specific_date.json",
			},
			{
				req: &pbv3.ObservationRequest{
					Variable: &pbv2.DcidOrExpression{
						Dcids: []string{"AirPollutant_Cancer_Risk"},
					},
					Entity: &pbv2.DcidOrExpression{
						Dcids: []string{"geoId/01001", "geoId/02013"},
					},
					Date: "1900",
				},
				goldenFile: "observations_no_matching_date.json",
			},
			{
				req: &pbv3.ObservationRequest{
					Variable: &pbv2.DcidOrExpression{
						Dcids: []string{"Count_Person", "Median_Age_Person"},
					},
					Entity: &pbv2.DcidOrExpression{
						Expression: "geoId/06<-containedInPlace+{typeOf:County}",
					},
					Date: "2015",
				},
				goldenFile: "observations_contained_in.json",
			},
		} {
			goldenFile := c.goldenFile
			resp, err := mixer.V3Observation(ctx, c.req)
			if err != nil {
				t.Errorf("Could not run V3Observation: %s", err)
				continue
			}
			if latencyTest {
				continue
			}
			if test.GenerateGolden {
				test.UpdateGolden(resp, goldenPath, goldenFile)
				continue
			}
			var expected pbv3.ObservationResponse
			if err = test.ReadJSON(goldenPath, goldenFile, &expected); err != nil {
				t.Errorf("Could not Unmarshal golden file: %s", err)
				continue
			}
			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("%s: got diff: %s", goldenFile, diff)
				continue
			}
		}
	}
	if err := test.TestDriver(
		"TestV3Observation",
		&test.TestOption{UseSQLite: true, UseSpannerGraph: true},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() for TestV3Observation = %s", err)
	}
}
