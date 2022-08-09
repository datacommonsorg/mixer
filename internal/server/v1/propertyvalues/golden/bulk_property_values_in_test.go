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

	testSuite := func(mixer pb.MixerClient, recon pb.ReconClient, latencyTest bool) {
		for _, c := range []struct {
			goldenFile string
			property   string
			entities   []string
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
				"H4sIAAAAAAAA/4TQwUr0MBDA8W2/UtLst1Bz6slX2PWgJ0Frdl3qQQp1EbxITIcaSBNIp0jfXrq9CNr0OIf/j5mhr3QtbW/QDdtTldNUWoNCGagLU2ohgV5yMF3fcevgQXRQVyhQdaik0LkDwVYsIkEasIiEacBC8o9FJEoD+rgEb7ht2x6Vad6sgVnnacm5OII9OlWfp/erPTSz1mHJ+l+UnE/Q9W6WySlpwBb1dnfzh7GeHvbihMRZ4t5LRFzhwFYsHtssYfFYZ8m5j8c+S+izV8gOGlowKNxQyU9r9V516JRno1uvR0v7Ba7UwoxCSMYy/FHfeetN2X9oJU+o9HTYL+AbAAD//wEAAP//xE8WrogCAAA=",
			},
			{
				"typeOf.json",
				"typeOf",
				[]string{"Country", "State", "City"},
				100,
				"",
			},
		} {
			req := &pb.BulkPropertyValuesRequest{
				Property:  c.property,
				Entities:  c.entities,
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
			var expected pb.BulkPropertyValuesResponse
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
