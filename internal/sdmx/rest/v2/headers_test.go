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
	"context"
	"strings"
	"testing"

	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestShouldLogSDMX(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		want bool
	}{
		{
			name: "missing metadata",
			ctx:  context.Background(),
		},
		{
			name: "missing header",
			ctx:  metadata.NewIncomingContext(context.Background(), metadata.Pairs("other", "true")),
		},
		{
			name: "false",
			ctx:  metadata.NewIncomingContext(context.Background(), metadata.Pairs(util.XLogSDMX, "false")),
		},
		{
			name: "other value",
			ctx:  metadata.NewIncomingContext(context.Background(), metadata.Pairs(util.XLogSDMX, "TRUE")),
		},
		{
			name: "true",
			ctx:  metadata.NewIncomingContext(context.Background(), metadata.Pairs(util.XLogSDMX, "true")),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ShouldLogSDMX(tt.ctx); got != tt.want {
				t.Errorf("ShouldLogSDMX() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataResponseFormatFromMetadata(t *testing.T) {
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
			ctx := context.Background()
			if tt.accept != "" {
				ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("accept", tt.accept))
			}

			got, err := DataResponseFormatFromMetadata(ctx)
			if tt.wantCode != codes.OK {
				if status.Code(err) != tt.wantCode {
					t.Fatalf("DataResponseFormatFromMetadata() code = %v, want %v; err = %v", status.Code(err), tt.wantCode, err)
				}
				if !strings.Contains(status.Convert(err).Message(), tt.wantErrSub) {
					t.Fatalf("DataResponseFormatFromMetadata() message = %q, want substring %q", status.Convert(err).Message(), tt.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("DataResponseFormatFromMetadata() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("DataResponseFormatFromMetadata() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAvailabilityResponseFormatFromMetadata(t *testing.T) {
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
			ctx := context.Background()
			if tt.accept != "" {
				ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("accept", tt.accept))
			}

			_, err := AvailabilityResponseFormatFromMetadata(ctx)
			if tt.wantCode != codes.OK {
				if status.Code(err) != tt.wantCode {
					t.Fatalf("AvailabilityResponseFormatFromMetadata() code = %v, want %v; err = %v", status.Code(err), tt.wantCode, err)
				}
				if !strings.Contains(status.Convert(err).Message(), tt.wantErrSub) {
					t.Fatalf("AvailabilityResponseFormatFromMetadata() message = %q, want substring %q", status.Convert(err).Message(), tt.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("AvailabilityResponseFormatFromMetadata() error = %v", err)
			}
		})
	}
}
