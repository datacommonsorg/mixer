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
			name:        "single value",
			originalURI: "/sdmx/v3/data?c[FREQ]=A",
			want:        map[string][]string{"FREQ": {"A"}},
		},
		{
			name:        "encoded name",
			originalURI: "/sdmx/v3/data?c%5BFREQ%5D=A",
			want:        map[string][]string{"FREQ": {"A"}},
		},
		{
			name:        "comma values",
			originalURI: "/sdmx/v3/data?c[FREQ]=A,M",
			want:        map[string][]string{"FREQ": {"A", "M"}},
		},
		{
			name:        "encoded slash value",
			originalURI: "/sdmx/v3/data?c[geo]=country%2FUSA",
			want:        map[string][]string{"geo": {"country/USA"}},
		},
		{
			name:        "encoded ampersand stays in value",
			originalURI: "/sdmx/v3/data?c[TITLE]=foo%26bar&c[FREQ]=A",
			want: map[string][]string{
				"TITLE": {"foo&bar"},
				"FREQ":  {"A"},
			},
		},
		{
			name:        "encoded equals stays in value",
			originalURI: "/sdmx/v3/data?c[TITLE]=foo%3Dbar",
			want:        map[string][]string{"TITLE": {"foo=bar"}},
		},
		{
			name:        "ordinary query parameters are ignored",
			originalURI: "/sdmx/v3/data?dimensionAtObservation=AllDimensions&c[FREQ]=A",
			want:        map[string][]string{"FREQ": {"A"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDataRequest("", tt.originalURI)
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
		"dataflow/DATACOMMONS/DF_OBSERVATIONS/1.0/*",
		"/sdmx/v3/data/dataflow/DATACOMMONS/DF_OBSERVATIONS/1.0/*?c[FREQ]=A",
	)
	if err != nil {
		t.Fatalf("ParseDataRequest() error = %v", err)
	}

	want := ResourcePath{
		Context:    "dataflow",
		AgencyID:   "DATACOMMONS",
		ResourceID: "DF_OBSERVATIONS",
		Version:    "1.0",
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
			originalURI: "/sdmx/v3/data?c[FREQ]=A+M",
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "encoded plus unsupported",
			originalURI: "/sdmx/v3/data?c[FREQ]=A%2BM",
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "operator unsupported",
			originalURI: "/sdmx/v3/data?c[TIME_PERIOD]=ge:2020",
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "duplicate component",
			originalURI: "/sdmx/v3/data?c[FREQ]=A&c[FREQ]=M",
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
			originalURI: "/sdmx/v3/data?c[FREQ]=%ZZ",
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "empty value",
			originalURI: "/sdmx/v3/data?c[FREQ]=",
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "non star key unsupported",
			tail:        "dataflow/DATACOMMONS/DF_OBSERVATIONS/1.0/A.US",
			originalURI: "/sdmx/v3/data/dataflow/DATACOMMONS/DF_OBSERVATIONS/1.0/A.US?c[FREQ]=A",
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
			tail:        "dataflow//DF_OBSERVATIONS/1.0/*",
			originalURI: "/sdmx/v3/data/dataflow//DF_OBSERVATIONS/1.0/*?c[FREQ]=A",
			wantCode:    codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseDataRequest(tt.tail, tt.originalURI)
			if err == nil {
				t.Fatal("ParseDataRequest() error = nil, want error")
			}
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("ParseDataRequest() code = %v, want %v; err = %v", got, tt.wantCode, err)
			}
		})
	}
}
