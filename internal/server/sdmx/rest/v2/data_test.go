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
	"strings"
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
			name:        "encoded slash value",
			originalURI: dataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"),
			want: map[string][]string{
				"variableMeasured": {"Count_Person"},
				"observationAbout": {"country/USA"},
			},
		},
		{
			name:        "dynamic entity filters",
			originalURI: dataURI("c[variableMeasured]=Count_Person_Migrated&c[destinationCountry]=country%2FCAN&c[sourceCountry]=country%2FUSA"),
			want: map[string][]string{
				"variableMeasured":   {"Count_Person_Migrated"},
				"destinationCountry": {"country/CAN"},
				"sourceCountry":      {"country/USA"},
			},
		},
		{
			name:        "filterable attribute",
			originalURI: dataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA&c[facetId]=facet"),
			want: map[string][]string{
				"variableMeasured": {"Count_Person"},
				"observationAbout": {"country/USA"},
				"facetId":          {"facet"},
			},
		},
		{
			name: "multiple values for fixed and dynamic dimensions",
			originalURI: dataURI("c[variableMeasured]=Count_Person_Migrated,Count_Refugee&" +
				"c[destinationCountry]=country%2FCAN,country%2FMEX&" +
				"c[sourceCountry]=country%2FUSA,country%2FIND&" +
				"c[unit]=Person,Traveler&" +
				"c[measurementMethod]=Census,Survey&" +
				"c[observationPeriod]=P1Y,P1M&" +
				"c[provenance]=dc%2Fbase%2Fone,dc%2Fbase%2Ftwo"),
			want: map[string][]string{
				"variableMeasured":   {"Count_Person_Migrated", "Count_Refugee"},
				"destinationCountry": {"country/CAN", "country/MEX"},
				"sourceCountry":      {"country/USA", "country/IND"},
				"unit":               {"Person", "Traveler"},
				"measurementMethod":  {"Census", "Survey"},
				"observationPeriod":  {"P1Y", "P1M"},
				"provenance":         {"dc/base/one", "dc/base/two"},
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
		AgencyID:   "DC",
		ResourceID: "DF_OBS",
		Version:    "1.0.0",
		Key:        "*",
	}
	if diff := cmp.Diff(want, got.Path); diff != "" {
		t.Errorf("ParseDataRequest() path mismatch (-want +got):\n%s", diff)
	}
}

func TestParseDataRequest_Format(t *testing.T) {
	tests := []struct {
		name        string
		originalURI string
		want        string
	}{
		{
			name:        "missing format",
			originalURI: dataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"),
		},
		{
			name:        "csv",
			originalURI: dataURI("format=csv&c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"),
			want:        "csv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDataRequest(dataTail(), tt.originalURI)
			if err != nil {
				t.Fatalf("ParseDataRequest() error = %v", err)
			}
			if got.Format != tt.want {
				t.Errorf("ParseDataRequest() format = %q, want %q", got.Format, tt.want)
			}
		})
	}
}

func TestDataResponseFormatFromDataRequest(t *testing.T) {
	tests := []struct {
		name       string
		request    *DataRequest
		accept     string
		want       DataResponseFormat
		wantCode   codes.Code
		wantErrSub string
	}{
		{
			name:       "format json stat unsupported",
			request:    &DataRequest{Format: "json-stat"},
			want:       DataResponseFormatUnknown,
			wantCode:   codes.InvalidArgument,
			wantErrSub: "unsupported SDMX data response format",
		},
		{
			name:    "format csv",
			request: &DataRequest{Format: "csv"},
			want:    DataResponseFormatCSV,
		},
		{
			name:    "format overrides accept",
			request: &DataRequest{Format: "csv"},
			accept:  "application/vnd.sdmx.data+json;version=2.0.0",
			want:    DataResponseFormatCSV,
		},
		{
			name:    "missing format falls back to accept",
			request: &DataRequest{},
			accept:  "application/vnd.sdmx.data+csv;version=2.0.0",
			want:    DataResponseFormatCSV,
		},
		{
			name:       "unsupported format",
			request:    &DataRequest{Format: "xml"},
			want:       DataResponseFormatUnknown,
			wantCode:   codes.InvalidArgument,
			wantErrSub: "unsupported SDMX data response format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			accept := []string(nil)
			if tt.accept != "" {
				accept = []string{tt.accept}
			}

			got, err := DataResponseFormatFromDataRequest(tt.request, accept)
			if tt.wantCode != codes.OK {
				if status.Code(err) != tt.wantCode {
					t.Fatalf("DataResponseFormatFromDataRequest() code = %v, want %v; err = %v", status.Code(err), tt.wantCode, err)
				}
				if !strings.Contains(status.Convert(err).Message(), tt.wantErrSub) {
					t.Fatalf("DataResponseFormatFromDataRequest() message = %q, want substring %q", status.Convert(err).Message(), tt.wantErrSub)
				}
				if got != tt.want {
					t.Errorf("DataResponseFormatFromDataRequest() = %v, want %v", got, tt.want)
				}
				return
			}
			if err != nil {
				t.Fatalf("DataResponseFormatFromDataRequest() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("DataResponseFormatFromDataRequest() = %v, want %v", got, tt.want)
			}
		})
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
			name:        "time period filter unsupported",
			originalURI: dataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA&c[TIME_PERIOD]=2020"),
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
			name:        "malformed component identifier",
			originalURI: dataURI("c[variableMeasured]=Count_Person&c[invalid%20key]=value"),
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
			tail:        "dataflow/DC/DF_OBS/1.0.0/A.US",
			originalURI: "/sdmx/v3/data/dataflow/DC/DF_OBS/1.0.0/A.US?c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA",
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "invalid path",
			tail:        "dataflow/DC",
			originalURI: "/sdmx/v3/data/dataflow/DC?c[FREQ]=A",
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "empty path segment",
			tail:        "dataflow//DF_OBS/1.0.0/*",
			originalURI: "/sdmx/v3/data/dataflow//DF_OBS/1.0.0/*?c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA",
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "missing path",
			originalURI: "/sdmx/v3/data?c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA",
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "wrong version",
			tail:        "dataflow/DC/DF_OBS/1.0/*",
			originalURI: "/sdmx/v3/data/dataflow/DC/DF_OBS/1.0/*?c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA",
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "missing variable measured",
			originalURI: dataURI("c[observationAbout]=country%2FUSA"),
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "freq unsupported",
			originalURI: dataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA&c[FREQ]=A"),
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "scaling factor unsupported",
			originalURI: dataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA&c[scalingFactor]=0"),
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "observation value unsupported",
			originalURI: dataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA&c[OBS_VALUE]=10"),
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "duplicate format",
			originalURI: dataURI("format=csv&format=json-stat&c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"),
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "unsupported format",
			originalURI: dataURI("format=xml&c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"),
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "json stat format unsupported",
			originalURI: dataURI("format=json-stat&c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"),
			wantCode:    codes.InvalidArgument,
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
	return "dataflow/DC/DF_OBS/1.0.0/*"
}

func dataURI(query string) string {
	return "/sdmx/v3/data/" + dataTail() + "?" + query
}
