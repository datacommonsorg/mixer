// Copyright 2023 Google LLC
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

func TestResolveDescription(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "resolve_description")

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			nodes      []string
			property   string
			goldenFile string
		}{
			{
				[]string{"Santa Clara County", "San Mateo County"},
				"<-description->dcid",
				"county.json",
			},
			{
				[]string{"Mountain View", "New York City"},
				"<-description{typeOf:City}->dcid",
				"city.json",
			},
			{
				[]string{
					"Brussels,Belgium",
					"Atacama Desert,Chile",
					"Sjælland,Denmark",
					"Athos,Greece",
					"Illes Balears,Spain",
					"Ciudad Autónoma de Ceuta,Spain",
					"Ciudad Autónoma de Melilla,Spain",
					"Corse,France",
					"Delhi,India",
					"Dadra And Nagar Haveli,India",
					"Bihar,India",
					"Hokkaido,Japan",
					"Tohoku,Japan",
					"Northern-Kanto,Japan",
					"Southern-Kanto,Japan",
					"Hokuriku,Japan",
					"Toukai,Japan",
					"Kansai,Japan",
					"Chugoku,Japan",
					"Shikoku,Japan",
					"Kyushu,Japan",
					"Loreto,Peru",
					"Małopolskie,Poland",
					"Pomorskie,Poland",
					"Tambov,Russia",
					"Arkhangelsk,Russia",
					"Vologda,Russia",
					"Republic of Crimea,Russia",
					"Astrakhan,Russia",
					"Ulyanovsk,Russia",
					"Kemerovo,Russia",
					"Kamchatka,Russia",
					"Sakhalin,Russia",
					"North East and Yorkshire,United Kingdom",
					"Midlands,United Kingdom",
					"East of England,United Kingdom",
					"Free State,South Africa",
					"Gauteng,South Africa",
					"KwaZulu-Natal,South Africa",
					"Ladakh,India",
				},
				"<-description->dcid",
				"harvard.json",
			},
			{
				[]string{
					"California, USA",
					"Mountain View, US",
					"Sunnyvale CA",
					"CA US",
					"ME",
				},
				"<-description->dcid",
				"abbreviated_names.json",
			},
			{
				[]string{
					"Cash",
					"cash",
					"Middle Point",
					"Middle Point, USA",
				},
				"<-description->dcid",
				"bogus_names.json",
			},
		} {
			resp, err := mixer.V2Resolve(ctx, &pbv2.ResolveRequest{
				Nodes:    c.nodes,
				Property: c.property,
			})
			if err != nil {
				t.Errorf("could not Resolve Description: %s", err)
				continue
			}

			if latencyTest {
				continue
			}

			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}

			var expected pbv2.ResolveResponse
			if err = test.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file")
				continue
			}

			cmpOpts := cmp.Options{protocmp.Transform()}
			if diff := cmp.Diff(resp, &expected, cmpOpts); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}

	if err := test.TestDriver(
		"Resolve(Description)", &test.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
