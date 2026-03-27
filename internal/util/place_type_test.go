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

package util

import (
	"testing"
)

func TestGetDominantType(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected string
	}{
		{
			name:     "Empty input",
			input:    []string{},
			expected: "",
		},
		{
			name:     "Single match",
			input:    []string{"City"},
			expected: "City",
		},
		{
			name:     "Multiple matches, first priority wins (Country vs State)",
			input:    []string{"State", "Country"},
			expected: "Country", // Country priority is defined as higher in our JSON subset
		},
		{
			name:     "Multiple matches, first priority wins (County vs City)",
			input:    []string{"City", "County"},
			expected: "County", // County is higher priority
		},
		{
			name:     "No matches in priority list",
			input:    []string{"UnknownType"},
			expected: "",
		},
		{
			name:     "Mixed types with match",
			input:    []string{"UnknownType", "Village", "Town"},
			expected: "Town", // Town is higher priority than Village in our JSON
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := GetDominantType(tc.input)
			if got != tc.expected {
				t.Errorf("GetDominantType(%v) = %v; want %v", tc.input, got, tc.expected)
			}
		})
	}
}
