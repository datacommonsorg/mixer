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

	testSuite := func(mixer pb.MixerClient, latencyTest bool) {
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
				"H4sIAAAAAAAA/4zSX6vTMBgGcPuXnJxzpOaqV36FbYJeijPTURxarPPCmxHblxnIEkjTyb72PoE07WRunmQ3LWnpr0/yvHiD72vVSaMPk3U1x1mtpGFcQlPIUrAa8EsKsu1aqjS8Zy00lWGGt4bXTMw1MBJmAYlRkAUkRCGJUWTXsb0mWYA/+37wSNVu1xkutz+UBK9X+rwXS1BLzRu72swWsL0wQxT9I37yiQ9FSenAvZ56sC/e7VavKAixgj2I2ZRE+TEiIRqwBNnl4CUoyY8RLjDagiqayfTNf7T7oZ1vmtXGe3QrJxVTbg4kzoL8jqS9kt9ZJ+0d++z0Lunv+LtTyz8I2IE0TB+q+pdSYsFbo/kNKd85XVyq36BLwaQhz/4eXN9CiOJRWDqFx1LzPTMwxLJxzpnzKB+d0EPZ/RS8vsGhnkDWWRsu+gae2pV7EC7G6jzN9RG/dVLpuKPrIEP1Af7q/P75ReH92KR95XlxypMX4zilo5n+AQAA//8BAAD//zL9sQaJBAAA",
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
			req := &pb.BulkPropertyValuesRequest{
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
