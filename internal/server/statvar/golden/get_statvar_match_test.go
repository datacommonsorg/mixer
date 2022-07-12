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
	"fmt"
	"path"
	"runtime"
	"strings"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

type QueryTestDefinition struct {
	propertyValues map[string]string
	goldenFile     string
}

func buildQuery(c QueryTestDefinition) string {
	var sb strings.Builder
	for key, value := range c.propertyValues {
		sb.WriteString(fmt.Sprintf("%s %s ", key, value))
	}
	return sb.String()
}

func TestGetStatVarMatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "get_statvar_match")

	testSuite := func(mixer pb.MixerClient, recon pb.ReconClient, latencyTest bool) {
		for _, c := range []QueryTestDefinition{
			{
				map[string]string{
					"gender":   "Female",
					"mp":       "count",
					"nativity": "USC_ForeignBorn",
				},
				"female_usc_foreignborn.json",
			},
			{
				map[string]string{
					"mp":     "count",
					"pt":     "Person",
					"gender": "Female",
				},
				"female.json",
			},
			{
				map[string]string{
					"mp":    "count",
					"pt":    "USCEstablishment",
					"st":    "measuredValue",
					"naics": "NAICS/71",
				},
				"energy_in_us.json",
			},
			{
				map[string]string{
					"mp":    "count",
					"pt":    "USCEstablishment",
					"st":    "measuredValue",
					"naics": "NAICS/71",
				},
				"energy_in_us_noquery.json",
			},
			{
				map[string]string{},
				"energy_in_us_nomodel.json",
			},
		} {
			resp, err := mixer.GetStatVarMatch(ctx, &pb.GetStatVarMatchRequest{
				Query: buildQuery(c),
				Debug: true,
			})
			if err != nil {
				t.Errorf("could not GetStatVarMatch: %s", err)
				continue
			}

			if latencyTest {
				continue
			}

			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}

			var expected pb.GetStatVarMatchResponse
			if err = test.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file")
				continue
			}

			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}

	if err := test.TestDriver(
		"GetStatVarMatch",
		&test.TestOption{UseCache: true, SearchOptions: server.SearchOptions{
			UseSearch:           true,
			BuildSvgSearchIndex: false,
			BuildSqliteIndex:    true,
		}},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
