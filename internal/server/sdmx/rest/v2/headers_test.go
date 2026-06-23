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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDataResponseFormatFromAccept(t *testing.T) {
	tests := []struct {
		name       string
		accept     string
		want       DataResponseFormat
		wantCode   codes.Code
		wantErrSub string
	}{
		{
			name: "missing metadata defaults to JSON-stat",
			want: DataResponseFormatJSONStat,
		},
		{
			name:   "JSON accept defaults to JSON-stat",
			accept: "application/json",
			want:   DataResponseFormatJSONStat,
		},
		{
			name:   "SDMX CSV",
			accept: "application/vnd.sdmx.data+csv;version=2.0.0",
			want:   DataResponseFormatCSV,
		},
		{
			name:   "text CSV",
			accept: "text/csv",
			want:   DataResponseFormatCSV,
		},
		{
			name:       "SDMX JSON not implemented",
			accept:     "application/vnd.sdmx.data+json;version=2.0.0",
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX JSON responses are not implemented yet",
		},
		{
			name:       "SDMX CSV option not implemented",
			accept:     "application/vnd.sdmx.data+csv;version=2.0.0;labels=name",
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX CSV response option",
		},
		{
			name:       "SDMX CSV version not implemented",
			accept:     "application/vnd.sdmx.data+csv;version=1.0.0",
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX CSV version",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			accept := []string(nil)
			if tt.accept != "" {
				accept = []string{tt.accept}
			}

			got, err := DataResponseFormatFromAccept(accept)
			if tt.wantCode != codes.OK {
				if status.Code(err) != tt.wantCode {
					t.Fatalf("DataResponseFormatFromAccept() code = %v, want %v; err = %v", status.Code(err), tt.wantCode, err)
				}
				if !strings.Contains(status.Convert(err).Message(), tt.wantErrSub) {
					t.Fatalf("DataResponseFormatFromAccept() message = %q, want substring %q", status.Convert(err).Message(), tt.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("DataResponseFormatFromAccept() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("DataResponseFormatFromAccept() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAvailabilityResponseFormatFromAccept(t *testing.T) {
	tests := []struct {
		name       string
		accept     string
		wantCode   codes.Code
		wantErrSub string
	}{
		{
			name: "missing metadata defaults to structure JSON",
		},
		{
			name:   "wildcard defaults to structure JSON",
			accept: "*/*",
		},
		{
			name:   "structure JSON 2.0",
			accept: "application/vnd.sdmx.structure+json;version=2.0.0",
		},
		{
			name:   "supported type after unsupported type",
			accept: "application/vnd.sdmx.structure+xml;version=3.0.0, application/vnd.sdmx.structure+json;version=2.0.0",
		},
		{
			name:       "structure JSON 2.1 not implemented",
			accept:     "application/vnd.sdmx.structure+json;version=2.1.0",
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX structure JSON version",
		},
		{
			name:       "structure XML not implemented",
			accept:     "application/vnd.sdmx.structure+xml;version=3.0.0",
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX structure XML responses are not implemented yet",
		},
		{
			name:       "CSV not implemented",
			accept:     "application/vnd.sdmx.data+csv;version=2.0.0",
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX availability response media type",
		},
		{
			name:       "data JSON not implemented",
			accept:     "application/vnd.sdmx.data+json;version=2.0.0",
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX availability response media type",
		},
		{
			name:       "application JSON not implemented",
			accept:     "application/json",
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX availability response media type",
		},
		{
			name:       "unsupported option",
			accept:     "application/vnd.sdmx.structure+json;version=2.0.0;labels=name",
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX structure JSON response option",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			accept := []string(nil)
			if tt.accept != "" {
				accept = []string{tt.accept}
			}

			_, err := AvailabilityResponseFormatFromAccept(accept)
			if tt.wantCode != codes.OK {
				if status.Code(err) != tt.wantCode {
					t.Fatalf("AvailabilityResponseFormatFromAccept() code = %v, want %v; err = %v", status.Code(err), tt.wantCode, err)
				}
				if !strings.Contains(status.Convert(err).Message(), tt.wantErrSub) {
					t.Fatalf("AvailabilityResponseFormatFromAccept() message = %q, want substring %q", status.Convert(err).Message(), tt.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("AvailabilityResponseFormatFromAccept() error = %v", err)
			}
		})
	}
}
