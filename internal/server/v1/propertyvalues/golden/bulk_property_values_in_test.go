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
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestBulkPropertyValuesIn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "bulk_property_values_in")

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			goldenFile string
			property   string
			nodes      []string
			limit      int32
			token      string
		}{
			{
				"containedIn1.json",
				"containedInPlace",
				[]string{"country/USA", "geoId/06053", "geoId/06"},
				0,
				"",
			},
			{
				"containedIn2.json",
				"containedInPlace",
				[]string{"country/USA", "geoId/06053", "geoId/06"},
				0,
				"H4sIAAAAAAAA/7SU0WoUMRRA3bhu02yr2zwtCP5CW0Hf67TWBYWB2YKu2JJNrrMXsrmSyVT2A/w9v0m28yaSkEgf5+Wcy5ncK0hMNfUu+N3pTXMhZppcUOjALFxtlQbxqgLX9V1FHt6pDkwTVMAuoFb2woOSTyTjI8k4k4w/lYyPJePPJOMTyfiBZJxLxg8l40KO+XQ2EjYlfDkIV/ijIgNLte6tCkiuyHabsh1XtN32AV27IgfZ/HWKf3INdO3RPHzdnV9Cm+34lnIcLeqqGgRvzh4hUfO6Ams/wj3Y83z+F8FboIU5PXv7D/h0+NtLr3TIRi+j6HGFYZfBnOyZ80NxG6XOrixswQXld43eENnsqb9H+fO/+ZfYBY8Fde6inpP3ROaqC2ptsdvsjdmCz1GB+IDtpjDRKko++oTGWChkJ6au6Sf42qqCHvGHc1x7vFchf+wDPp2x+a/9IYhWqfu1RV2EH81/M/E1Mf4D/iagzVuroU0C/n8nJh7+efEeDRdhkgj/ogFNzhQfhD8AAAD//wEAAP//GGx0v4QHAAA=",
			},
			{
				"typeOf.json",
				"typeOf",
				[]string{"Country", "State", "City"},
				100,
				"",
			},
			{
				"nasa_source.json",
				"isPartOf",
				[]string{"dc/s/UsNationalAeronauticsAndSpaceAdministrationNasa"},
				500,
				"",
			},
		} {
			req := &pbv1.BulkPropertyValuesRequest{
				Property:  c.property,
				Nodes:     c.nodes,
				Direction: util.DirectionIn,
				Limit:     c.limit,
				NextToken: c.token,
			}
			resp, err := mixer.BulkPropertyValues(ctx, req)
			if err != nil {
				t.Errorf("could not run mixer.BulkPropertyValues/in: %s", err)
				continue
			}
			if latencyTest {
				continue
			}
			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}
			var expected pbv1.BulkPropertyValuesResponse
			if err := test.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file %s: %v", c.goldenFile, err)
				continue
			}
			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("Golden got diff: %v", diff)
				continue
			}
		}
	}
	if err := test.TestDriver(
		"BulkPropertyValues/in", &test.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
