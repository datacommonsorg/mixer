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

package jsonv2

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestAvailabilityJSONFormatter_Format(t *testing.T) {
	formatter := &AvailabilityJSONFormatter{}

	got, err := formatter.Format("observationAbout", []string{"country/USA", "geoId/06"})
	if err != nil {
		t.Fatalf("Format() error = %v", err)
	}

	assertAvailabilityGolden(t, "availability_observation_about.json", got)
}

func TestAvailabilityJSONFormatter_EmptyValues(t *testing.T) {
	formatter := &AvailabilityJSONFormatter{}

	got, err := formatter.Format("TIME_PERIOD", nil)
	if err != nil {
		t.Fatalf("Format() error = %v", err)
	}

	assertAvailabilityGolden(t, "availability_time_period_empty.json", got)
}

func TestAvailabilityJSONFormatter_MultipleComponents(t *testing.T) {
	formatter := &AvailabilityJSONFormatter{}

	got, err := formatter.FormatComponents([]AvailabilityComponentValues{
		{ID: "observationAbout", Values: []string{"country/USA", "geoId/06"}},
		{ID: "TIME_PERIOD", Values: []string{"2020", "2021"}},
	})
	if err != nil {
		t.Fatalf("FormatComponents() error = %v", err)
	}

	assertAvailabilityGolden(t, "availability_multiple_components.json", got)
}

func assertAvailabilityGolden(t *testing.T, goldenFile string, got string) {
	t.Helper()

	var gotMap map[string]interface{}
	if err := json.Unmarshal([]byte(got), &gotMap); err != nil {
		t.Fatalf("Failed to unmarshal output: %v", err)
	}

	goldenPath := filepath.Join("golden", goldenFile)
	if os.Getenv("UPDATE_GOLDEN") == "true" {
		b, err := json.MarshalIndent(gotMap, "", "  ")
		if err != nil {
			t.Fatalf("Failed to marshal golden output: %v", err)
		}
		b = append(b, '\n')
		if err := os.WriteFile(goldenPath, b, 0644); err != nil {
			t.Fatalf("Failed to write golden file: %v", err)
		}
		return
	}

	goldenBytes, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("Failed to read golden file %q: %v", goldenPath, err)
	}

	var wantMap map[string]interface{}
	if err := json.Unmarshal(goldenBytes, &wantMap); err != nil {
		t.Fatalf("Failed to unmarshal golden file: %v", err)
	}

	if diff := cmp.Diff(wantMap, gotMap); diff != "" {
		t.Errorf("Formatter output mismatch (-want +got):\n%s", diff)
	}
}
