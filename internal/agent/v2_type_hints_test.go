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

package agent

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestTypesForHints(t *testing.T) {
	typesByHint := map[string][]typeCandidate{
		"school":     {{"School", 0.7}, {"PublicSchool", 0.69}, {"HighSchool", 0.68}, {"PrivateSchool", 0.67}},
		"university": {{"CollegeOrUniversity", 0.8}, {"School", 0.6}},
	}
	tests := []struct {
		name  string
		hints []string
		want  []string
	}{
		{
			name:  "keeps the top v2MaxTypesPerHint classes per hint",
			hints: []string{"school"},
			want:  []string{"School", "PublicSchool", "HighSchool"},
		},
		{
			name:  "deduplicates across hints in hint order",
			hints: []string{"university", " school "},
			want:  []string{"CollegeOrUniversity", "School", "PublicSchool", "HighSchool"},
		},
		{
			name:  "unknown hints contribute nothing",
			hints: []string{"spaceship"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if diff := cmp.Diff(tc.want, typesForHints(tc.hints, typesByHint)); diff != "" {
				t.Errorf("typesForHints() diff (-want +got):\n%s", diff)
			}
		})
	}
}
