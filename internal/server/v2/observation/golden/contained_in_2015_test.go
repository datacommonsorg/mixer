// Copyright 2022 Google LLC
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
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

const date = "2015"

func TestFetchContainIn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "contained_in_2015")

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			variables        []string
			entityExpression string
			filter           *pbv2.FacetFilter
			goldenFile       string
		}{
			{
				[]string{
					"test_var_1",
				},
				"country/USA<-containedInPlace+{typeOf:State}",
				&pbv2.FacetFilter{},
				"US_State.json",
			},
			{
				[]string{"Count_Person", "Median_Age_Person"},
				"geoId/06<-containedInPlace+{typeOf:County}",
				&pbv2.FacetFilter{},
				"CA_County.json",
			},
			{
				[]string{"Count_Person"},
				"country/FRA<-containedInPlace+{typeOf:AdministrativeArea2}",
				&pbv2.FacetFilter{},
				"FRA_AA2.json",
			},
			{
				[]string{"Count_Person"},
				"country/USA<-containedInPlace+{typeOf:State}",
				&pbv2.FacetFilter{
					Domain: "census.gov",
				},
				"filter.json",
			},
		} {
			goldenFile := c.goldenFile
			resp, err := mixer.V2Observation(ctx, &pbv2.ObservationRequest{
				Select:   []string{"variable", "entity", "date", "value"},
				Variable: &pbv2.DcidOrExpression{Dcids: c.variables},
				Entity:   &pbv2.DcidOrExpression{Expression: c.entityExpression},
				Date:     date,
				Filter:   c.filter,
			})
			if err != nil {
				t.Errorf("could not run V2Observation (contained_in): %s", err)
				continue
			}
			if latencyTest {
				continue
			}
			if test.GenerateGolden {
				test.UpdateGolden(resp, goldenPath, goldenFile)
				continue
			}
			var expected pbv2.ObservationResponse
			if err = test.ReadJSON(goldenPath, goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file: %s", err)
				continue
			}
			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}
	if err := test.TestDriver(
		"FetchContainIn",
		&test.TestOption{UseSQLite: true},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
