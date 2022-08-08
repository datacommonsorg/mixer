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

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestPropertyValuesIn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "property_values_in")

	testSuite := func(mixer pb.MixerClient, recon pb.ReconClient, latencyTest bool) {
		for _, c := range []struct {
			goldenFile string
			property   string
			entity     string
			limit      int32
			token      string
		}{
			// {
			// 	"containedIn1.json",
			// 	"containedInPlace",
			// 	"geoId/06",
			// 	502,
			// 	"",
			// },
			// {
			// 	"containedIn2.json",
			// 	"containedInPlace",
			// 	"geoId/06",
			// 	500,
			// 	"H4sIAAAAAAAA/4zUzU6rQBQH8Aul90642iBq0rfwY+HCj5gKSqtJQ0J1a6ZwhJNMZ5phaNM39jEM7UJjM6ddTVj8f5wPGP/BZyWoUXF2fuUHuZKGo4RiJFPBc/APxoBlNVW6UqoI/4QecwIn9JgbOKHLOqHHvMDxb0ijuz6s4ZgM9zLDDURqNlcSpLEqT6QSTLCAhDcltBwqaXXuSOffGwrBS3szz2T8JAJZN3WkGmlWMS6wpkoZkNb/jTXRPLdP5YUkTiMlSw11WwUXMdZGI4GNSaz/KGAG0nC9yvJKqd0evbLeltJlTv+zE3aZ2x77zukwa+agPxpZZGi+F+cy9wdxTRLeRC3taxqR2eNBMUPZNsENLmCggV9aqR1lRGhW1mxCZo8SUInGYv30fhFDaYXu6XGmzVRg/mpQbKpx2e9hDkkg3B6ItZQd0hDLas9v7ZaU/FQtQaeCry+Y7Y7o9N/N/2x79xcAAAD//wEAAP//XVK+mGIFAAA=",
			// },
			// {
			// 	"geoOverlaps.json",
			// 	"geoOverlaps",
			// 	"geoId/0649670",
			// 	500,
			// 	"",
			// },
			{
				"typeOf1.json",
				"typeOf",
				"Country",
				50,
				"",
			},
			{
				"typeOf2.json",
				"typeOf",
				"Country",
				0,
				"H4sIAAAAAAAA/+LS52J3zi/NKymq5GIrqSxI9U/jYnHOLKkUYhBi4WAUYBRi4WASYBRi4mAWYuFgEWDkMsLUABOA6JEwAumRMILpkTDissPUI+yYkpuZl1lcUpRYklmW6liUmmiI007i9Bvh0g8AAAD//wEAAP//jbVf3eUAAAA=",
			},
		} {
			req := &pb.PropertyValuesRequest{
				EntityProperty: c.entity + "/" + c.property,
				Direction:      "in",
				Limit:          c.limit,
				NextToken:      c.token,
			}
			resp, err := mixer.PropertyValues(ctx, req)
			if err != nil {
				t.Errorf("could not run mixer.PropertyValues/in: %s", err)
				continue
			}
			if latencyTest {
				continue
			}
			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}
			var expected pb.PropertyValuesResponse
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
		"PropertyValues/in", &test.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
