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
				"H4sIAAAAAAAA/6SW3U7bQBCFiWuKZX7kuq1UqVIfoQRaetE/GhxiQgpKSWglbqqNPXVWcnaj9RqUt69sQ0lwdiaoVwhF58vsmTMzcb+7m5HMhVaz3ctBy/UiKTTjAuKu6KcsAvd5K55wwTOtmObX0FLA9vw133YaXsO3Hctr+JbzxLcd22u43yjaRlB9bCR8JQlhyiIO6h/Bcqw5/S9K/yYAkeVZIBUcsQzigWaaZ5pHLC3edou1ysLuwZZ7TIG3KvAFJFwK4/s6FGY7kJNJrrlIrqQAYzmnFOdZCDJUPC7/+73XhuQ/ntbtB0EFOmgaMWTr1guvwWhNl9J7Qx5DyPIEyqYhLn9xNxOQ3Xi3+aF58G4Jyg64RkOIy9fLP0b9IaV3+/IGVD9lQvtrvuU8THGbAmwP8imoP7mIB3zO0kXKkKK8riJ7xaeBjGHIRnla+jo3CEvbhFNfHE/ZBUylKkLcYRFPK6/rz+yRqIUQN9/uH9zn+CEspGDeAux978wAIqvy6yvR6FZAwXZaXP3IWeHRbSvrPp2RPt0ttVzoWZtf8wybDjJdmxVuqFikkRw4d5TVzsY+UhCG2inXRiAnUylAmAtqoZSVJuYjirCH8gbbOZh24ydPU5aYd8YJKvdPeDIeRGMp03ZhKkcac4j70M9HKY8utWksLbIdy8qon4PPKAXfgJT6aZV0owXnqPrVcQoTEJqp2YpvOUJ5W+fAk/FIqrGUsbEmIltzB6n+/T1U+zKQIlGQFUPP6HyEKMz4u6EO+oSCiBvZQcWrn3pibh6xqk9R0uNWLL6LluzXWs//AgAA//8BAAD//w0SKrGaCwAA",
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
