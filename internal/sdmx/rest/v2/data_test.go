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

func TestParseDataRequest_Constraints(t *testing.T) {
	tests := []struct {
		name        string
		originalURI string
		want        map[string][]string
	}{
		{
			name:        "required filters",
			originalURI: dataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"),
			want: map[string][]string{
				"variableMeasured": {"Count_Person"},
				"observationAbout": {"country/USA"},
			},
		},
		{
			name:        "encoded name",
			originalURI: dataURI("c%5BvariableMeasured%5D=Count_Person&c%5BobservationAbout%5D=country%2FUSA"),
			want: map[string][]string{
				"variableMeasured": {"Count_Person"},
				"observationAbout": {"country/USA"},
			},
		},
		{
			name:        "optional time period",
			originalURI: dataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA&c[TIME_PERIOD]=2020,2021"),
			want: map[string][]string{
				"variableMeasured": {"Count_Person"},
				"observationAbout": {"country/USA"},
				"TIME_PERIOD":      {"2020", "2021"},
			},
		},
		{
			name:        "encoded slash value",
			originalURI: dataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"),
			want: map[string][]string{
				"variableMeasured": {"Count_Person"},
				"observationAbout": {"country/USA"},
			},
		},
		{
			name:        "encoded ampersand stays in value",
			originalURI: dataURI("c[variableMeasured]=Count%26Person&c[observationAbout]=country%2FUSA"),
			want: map[string][]string{
				"variableMeasured": {"Count&Person"},
				"observationAbout": {"country/USA"},
			},
		},
		{
			name:        "encoded equals stays in value",
			originalURI: dataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%3DUSA"),
			want: map[string][]string{
				"variableMeasured": {"Count_Person"},
				"observationAbout": {"country=USA"},
			},
		},
		{
			name:        "ordinary query parameters are ignored",
			originalURI: dataURI("dimensionAtObservation=AllDimensions&c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"),
			want: map[string][]string{
				"variableMeasured": {"Count_Person"},
				"observationAbout": {"country/USA"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDataRequest(dataTail(), tt.originalURI)
			if err != nil {
				t.Fatalf("ParseDataRequest() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, got.Constraints); diff != "" {
				t.Errorf("ParseDataRequest() constraints mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestParseDataRequest_Path(t *testing.T) {
	got, err := ParseDataRequest(
		dataTail(),
		dataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"),
	)
	if err != nil {
		t.Fatalf("ParseDataRequest() error = %v", err)
	}

	want := ResourcePath{
		Context:    "dataflow",
		AgencyID:   "DATACOMMONS",
		ResourceID: "DF_OBSERVATIONS",
		Version:    "1.0.0",
		Key:        "*",
	}
	if diff := cmp.Diff(want, got.Path); diff != "" {
		t.Errorf("ParseDataRequest() path mismatch (-want +got):\n%s", diff)
	}
}

func TestParseDataRequest_Errors(t *testing.T) {
	tests := []struct {
		name        string
		tail        string
		originalURI string
		wantCode    codes.Code
	}{
		{
			name:        "raw plus unsupported",
			originalURI: dataURI("c[variableMeasured]=Count+Person&c[observationAbout]=country%2FUSA"),
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "encoded plus unsupported",
			originalURI: dataURI("c[variableMeasured]=Count%2BPerson&c[observationAbout]=country%2FUSA"),
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "operator unsupported",
			originalURI: dataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA&c[TIME_PERIOD]=ge:2020"),
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "duplicate component",
			originalURI: dataURI("c[variableMeasured]=Count_Person&c[variableMeasured]=Count_Household&c[observationAbout]=country%2FUSA"),
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "empty component",
			originalURI: "/sdmx/v3/data?c[]=A",
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "malformed component",
			originalURI: "/sdmx/v3/data?c[FREQ=A",
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "bad name encoding",
			originalURI: "/sdmx/v3/data?c%ZZ=A",
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "bad value encoding",
			originalURI: dataURI("c[variableMeasured]=%ZZ&c[observationAbout]=country%2FUSA"),
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "empty value",
			originalURI: dataURI("c[variableMeasured]=&c[observationAbout]=country%2FUSA"),
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "non star key unsupported",
			tail:        "dataflow/DATACOMMONS/DF_OBSERVATIONS/1.0.0/A.US",
			originalURI: "/sdmx/v3/data/dataflow/DATACOMMONS/DF_OBSERVATIONS/1.0.0/A.US?c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA",
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "invalid path",
			tail:        "dataflow/DATACOMMONS",
			originalURI: "/sdmx/v3/data/dataflow/DATACOMMONS?c[FREQ]=A",
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "empty path segment",
			tail:        "dataflow//DF_OBSERVATIONS/1.0.0/*",
			originalURI: "/sdmx/v3/data/dataflow//DF_OBSERVATIONS/1.0.0/*?c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA",
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "missing path",
			originalURI: "/sdmx/v3/data?c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA",
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "wrong version",
			tail:        "dataflow/DATACOMMONS/DF_OBSERVATIONS/1.0/*",
			originalURI: "/sdmx/v3/data/dataflow/DATACOMMONS/DF_OBSERVATIONS/1.0/*?c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA",
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "missing variable measured",
			originalURI: dataURI("c[observationAbout]=country%2FUSA"),
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "missing observation about",
			originalURI: dataURI("c[variableMeasured]=Count_Person"),
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "geo unsupported",
			originalURI: dataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA&c[geo]=country%2FUSA"),
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "freq unsupported",
			originalURI: dataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA&c[FREQ]=A"),
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "observation value unsupported",
			originalURI: dataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA&c[OBS_VALUE]=10"),
			wantCode:    codes.Unimplemented,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tail := tt.tail
			if tail == "" && tt.name != "missing path" {
				tail = dataTail()
			}
			_, err := ParseDataRequest(tail, tt.originalURI)
			if err == nil {
				t.Fatal("ParseDataRequest() error = nil, want error")
			}
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("ParseDataRequest() code = %v, want %v; err = %v", got, tt.wantCode, err)
			}
		})
	}
}

func dataTail() string {
	return "dataflow/DATACOMMONS/DF_OBSERVATIONS/1.0.0/*"
}

func dataURI(query string) string {
	return "/sdmx/v3/data/" + dataTail() + "?" + query
}
