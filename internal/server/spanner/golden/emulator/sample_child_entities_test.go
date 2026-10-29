// Copyright 2026 Google LLC
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

package emulator

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSampleChildEntities(t *testing.T) {
	s := requireSuite(t)
	tests := []struct {
		name      string
		ancestors []string
		childType string
		limit     int
		want      map[string][]string
	}{
		{
			// The sample is in FARM_FINGERPRINT order, which is fixed, so the same request returns the same sample.
			name:      "samples per ancestor in fingerprint order, ancestors without children omitted",
			ancestors: []string{"northamerica", "europe", "bogus/1"},
			childType: "Country",
			limit:     2,
			want: map[string][]string{
				"northamerica": {"country/CAN", "country/USA"},
				"europe":       {"country/FRA", "country/GBR"},
			},
		},
		{
			name:      "a child with several provenance rows is returned once",
			ancestors: []string{"testRegion/duplicateProvenance"},
			childType: "Country",
			limit:     5,
			want:      map[string][]string{"testRegion/duplicateProvenance": {"country/CAN", "country/USA", "country/MEX"}},
		},
		{
			name:      "the limit applies after duplicate rows are dropped",
			ancestors: []string{"testRegion/duplicateProvenance"},
			childType: "Country",
			limit:     2,
			want:      map[string][]string{"testRegion/duplicateProvenance": {"country/CAN", "country/USA"}},
		},
		{
			name:      "a type without children",
			ancestors: []string{"northamerica"},
			childType: "State",
			limit:     5,
			want:      map[string][]string{},
		},
		{
			name:  "no ancestors",
			limit: 5,
			want:  map[string][]string{},
		},
	}
	for _, cfg := range childQueryConfigs {
		client, err := s.newSpannerClient(context.Background(), cfg.queryConfig)
		if err != nil {
			t.Fatalf("newSpannerClient(%s) error = %v", cfg.name, err)
		}
		defer client.Close()
		for _, tc := range tests {
			t.Run(cfg.name+"/"+tc.name, func(t *testing.T) {
				got, err := client.SampleChildEntities(context.Background(), tc.ancestors, tc.childType, tc.limit)
				if err != nil {
					t.Fatalf("SampleChildEntities() error = %v", err)
				}
				if diff := cmp.Diff(tc.want, got); diff != "" {
					t.Errorf("SampleChildEntities() mismatch (-want +got):\n%s", diff)
				}
			})
		}
	}
}
