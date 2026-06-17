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

package restv2

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestParseAvailabilityRequest_Constraints(t *testing.T) {
	tests := []struct {
		name        string
		tail        string
		originalURI string
		wantPath    AvailabilityPath
		want        map[string][]string
	}{
		{
			name:        "observation about",
			tail:        availabilityTail("observationAbout"),
			originalURI: availabilityURI("observationAbout", "c[variableMeasured]=Count_Person"),
			wantPath:    availabilityPath("observationAbout"),
			want:        map[string][]string{"variableMeasured": {"Count_Person"}},
		},
		{
			name:        "provenance",
			tail:        availabilityTail("provenance"),
			originalURI: availabilityURI("provenance", "c[variableMeasured]=Count_Person"),
			wantPath:    availabilityPath("provenance"),
			want:        map[string][]string{"variableMeasured": {"Count_Person"}},
		},
		{
			name:        "time period",
			tail:        availabilityTail("TIME_PERIOD"),
			originalURI: availabilityURI("TIME_PERIOD", "c[variableMeasured]=Count_Person"),
			wantPath:    availabilityPath("TIME_PERIOD"),
			want:        map[string][]string{"variableMeasured": {"Count_Person"}},
		},
		{
			name:        "or values",
			tail:        availabilityTail("observationAbout"),
			originalURI: availabilityURI("observationAbout", "c[variableMeasured]=Count_Person,Count_Household"),
			wantPath:    availabilityPath("observationAbout"),
			want:        map[string][]string{"variableMeasured": {"Count_Person", "Count_Household"}},
		},
		{
			name:        "optional dimension filter",
			tail:        availabilityTail("provenance"),
			originalURI: availabilityURI("provenance", "c[variableMeasured]=Count_Person&c[TIME_PERIOD]=2020,2021"),
			wantPath:    availabilityPath("provenance"),
			want: map[string][]string{
				"variableMeasured": {"Count_Person"},
				"TIME_PERIOD":      {"2020", "2021"},
			},
		},
		{
			name:        "mode and references defaults",
			tail:        availabilityTail("observationAbout"),
			originalURI: availabilityURI("observationAbout", "mode=exact&references=none&c[variableMeasured]=Count_Person"),
			wantPath:    availabilityPath("observationAbout"),
			want:        map[string][]string{"variableMeasured": {"Count_Person"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseAvailabilityRequest(tt.tail, tt.originalURI)
			if err != nil {
				t.Fatalf("ParseAvailabilityRequest() error = %v", err)
			}
			if diff := cmp.Diff(tt.wantPath, got.Path); diff != "" {
				t.Errorf("ParseAvailabilityRequest() path mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.want, got.Constraints); diff != "" {
				t.Errorf("ParseAvailabilityRequest() constraints mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestParseAvailabilityRequest_Errors(t *testing.T) {
	tests := []struct {
		name        string
		tail        string
		originalURI string
		wantCode    codes.Code
	}{
		{
			name:        "wrong version",
			tail:        "dataflow/DATACOMMONS/DF_OBSERVATIONS/1.0/*/observationAbout",
			originalURI: "/sdmx/v3/availability/dataflow/DATACOMMONS/DF_OBSERVATIONS/1.0/*/observationAbout?c[variableMeasured]=Count_Person",
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "non star key unsupported",
			tail:        "dataflow/DATACOMMONS/DF_OBSERVATIONS/1.0.0/A.US/observationAbout",
			originalURI: "/sdmx/v3/availability/dataflow/DATACOMMONS/DF_OBSERVATIONS/1.0.0/A.US/observationAbout?c[variableMeasured]=Count_Person",
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "missing component",
			tail:        dataTail(),
			originalURI: "/sdmx/v3/availability/dataflow/DATACOMMONS/DF_OBSERVATIONS/1.0.0/*?c[variableMeasured]=Count_Person",
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "missing variable measured",
			originalURI: availabilityURI("observationAbout", "c[observationAbout]=country%2FUSA"),
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "selected observation value unsupported",
			tail:        availabilityTail("OBS_VALUE"),
			originalURI: availabilityURI("OBS_VALUE", "c[variableMeasured]=Count_Person"),
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "selected attribute unsupported",
			tail:        availabilityTail("scalingFactor"),
			originalURI: availabilityURI("scalingFactor", "c[variableMeasured]=Count_Person"),
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "filter observation value unsupported",
			originalURI: availabilityURI("observationAbout", "c[variableMeasured]=Count_Person&c[OBS_VALUE]=10"),
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "filter attribute unsupported",
			originalURI: availabilityURI("observationAbout", "c[variableMeasured]=Count_Person&c[scalingFactor]=0"),
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "duplicate component",
			originalURI: availabilityURI("observationAbout", "c[variableMeasured]=Count_Person&c[variableMeasured]=Count_Household"),
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "raw plus unsupported",
			originalURI: availabilityURI("observationAbout", "c[variableMeasured]=Count+Person"),
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "encoded plus unsupported",
			originalURI: availabilityURI("observationAbout", "c[variableMeasured]=Count%2BPerson"),
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "operator unsupported",
			originalURI: availabilityURI("observationAbout", "c[variableMeasured]=ge:2020"),
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "available mode unsupported",
			originalURI: availabilityURI("observationAbout", "mode=available&c[variableMeasured]=Count_Person"),
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "references all unsupported",
			originalURI: availabilityURI("observationAbout", "references=all&c[variableMeasured]=Count_Person"),
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "updated after unsupported",
			originalURI: availabilityURI("observationAbout", "updatedAfter=2020-01-01&c[variableMeasured]=Count_Person"),
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "reporting year unsupported",
			originalURI: availabilityURI("observationAbout", "reportingYearStartDay=--01-01&c[variableMeasured]=Count_Person"),
			wantCode:    codes.Unimplemented,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tail := tt.tail
			if tail == "" {
				tail = availabilityTail("observationAbout")
			}
			_, err := ParseAvailabilityRequest(tail, tt.originalURI)
			if err == nil {
				t.Fatal("ParseAvailabilityRequest() error = nil, want error")
			}
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("ParseAvailabilityRequest() code = %v, want %v; err = %v", got, tt.wantCode, err)
			}
		})
	}
}

func availabilityTail(componentID string) string {
	return dataTail() + "/" + componentID
}

func availabilityURI(componentID string, query string) string {
	return "/sdmx/v3/availability/" + availabilityTail(componentID) + "?" + query
}

func availabilityPath(componentID string) AvailabilityPath {
	return AvailabilityPath{
		ResourcePath: ResourcePath{
			Context:    "dataflow",
			AgencyID:   "DATACOMMONS",
			ResourceID: "DF_OBSERVATIONS",
			Version:    "1.0.0",
			Key:        "*",
		},
		ComponentID: componentID,
	}
}
