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

package sdmxjsonv2

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

var availabilityTestTime = time.Date(2026, time.August, 10, 12, 34, 56, 0, time.UTC)

func TestAvailabilityJSONFormatter_Format(t *testing.T) {
	formatter := newTestAvailabilityJSONFormatter()

	got, err := formatter.Format("observationAbout", []string{"country/USA", "geoId/06"})
	if err != nil {
		t.Fatalf("Format() error = %v", err)
	}

	assertAvailabilityGolden(t, "availability_observation_about.json", got)
	assertAvailabilityComponentValues(t, got, []availabilityComponentValue{
		{Value: "country/USA"},
		{Value: "geoId/06"},
	})
}

func TestAvailabilityJSONFormatter_EmptyValues(t *testing.T) {
	formatter := newTestAvailabilityJSONFormatter()

	got, err := formatter.Format("TIME_PERIOD", nil)
	if err != nil {
		t.Fatalf("Format() error = %v", err)
	}

	assertAvailabilityGolden(t, "availability_time_period_empty.json", got)
}

func TestAvailabilityJSONFormatter_MultipleComponents(t *testing.T) {
	formatter := newTestAvailabilityJSONFormatter()

	got, err := formatter.FormatComponents([]AvailabilityComponentValues{
		{ID: "observationAbout", Values: []string{"country/USA", "geoId/06"}},
		{ID: "TIME_PERIOD", Values: []string{"2020", "2021"}},
	})
	if err != nil {
		t.Fatalf("FormatComponents() error = %v", err)
	}

	assertAvailabilityGolden(t, "availability_multiple_components.json", got)
}

func newTestAvailabilityJSONFormatter() *AvailabilityJSONFormatter {
	return &AvailabilityJSONFormatter{
		now: func() time.Time { return availabilityTestTime },
	}
}

func assertAvailabilityGolden(t *testing.T, goldenFile string, got string) {
	t.Helper()
	assertAvailabilityMeta(t, got)

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

func assertAvailabilityMeta(t *testing.T, got string) {
	t.Helper()

	if !strings.HasPrefix(got, `{"meta":`) {
		t.Errorf("Formatter output does not start with meta: %s", got)
	}

	var message availabilityMessage
	if err := json.Unmarshal([]byte(got), &message); err != nil {
		t.Fatalf("Failed to unmarshal output as availability message: %v", err)
	}
	want := availabilityMeta{
		Schema:   StructureJSONSchema,
		ID:       "DF_OBS_AVAILABILITY",
		Prepared: availabilityTestTime.Format(time.RFC3339),
		Sender:   availabilitySender{ID: "DC"},
	}
	if diff := cmp.Diff(want, message.Meta); diff != "" {
		t.Errorf("Availability meta mismatch (-want +got):\n%s", diff)
	}
	if _, err := time.Parse(time.RFC3339, message.Meta.Prepared); err != nil {
		t.Errorf("meta.prepared = %q, want RFC3339 timestamp: %v", message.Meta.Prepared, err)
	}
	if !strings.HasSuffix(message.Meta.Prepared, "Z") {
		t.Errorf("meta.prepared = %q, want UTC timestamp", message.Meta.Prepared)
	}

	var topLevel map[string]json.RawMessage
	if err := json.Unmarshal([]byte(got), &topLevel); err != nil {
		t.Fatalf("Failed to unmarshal top-level output: %v", err)
	}
	if _, found := topLevel["$schema"]; found {
		t.Errorf("Formatter output contains deprecated top-level $schema: %s", got)
	}
}

func assertAvailabilityComponentValues(t *testing.T, got string, want []availabilityComponentValue) {
	t.Helper()

	var message availabilityMessage
	if err := json.Unmarshal([]byte(got), &message); err != nil {
		t.Fatalf("Failed to unmarshal component values as objects: %v", err)
	}
	values := message.Data.DataConstraints[0].CubeRegions[0].KeyValues[0].Values
	if diff := cmp.Diff(want, values); diff != "" {
		t.Errorf("Availability component values mismatch (-want +got):\n%s", diff)
	}
}
