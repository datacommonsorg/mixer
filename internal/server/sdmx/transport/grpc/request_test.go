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

package grpc

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestOriginalURI(t *testing.T) {
	tests := []struct {
		name       string
		ctx        context.Context
		want       string
		wantCode   codes.Code
		wantErrSub string
	}{
		{
			name:       "missing metadata",
			ctx:        context.Background(),
			wantCode:   codes.InvalidArgument,
			wantErrSub: "missing SDMX request URI",
		},
		{
			name:       "missing URI headers",
			ctx:        metadata.NewIncomingContext(context.Background(), metadata.Pairs("other", "value")),
			wantCode:   codes.InvalidArgument,
			wantErrSub: "missing SDMX request URI",
		},
		{
			name: "original URI wins",
			ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs(
				"x-dc-original-uri", "/sdmx/v3/data/original",
				"x-envoy-original-path", "/sdmx/v3/data/envoy",
			)),
			want: "/sdmx/v3/data/original",
		},
		{
			name: "envoy fallback",
			ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs(
				"x-envoy-original-path", "/sdmx/v3/data/envoy",
			)),
			want: "/sdmx/v3/data/envoy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := OriginalURI(tt.ctx)
			if tt.wantCode != codes.OK {
				if status.Code(err) != tt.wantCode {
					t.Fatalf("OriginalURI() code = %v, want %v; err = %v", status.Code(err), tt.wantCode, err)
				}
				if !strings.Contains(status.Convert(err).Message(), tt.wantErrSub) {
					t.Fatalf("OriginalURI() message = %q, want substring %q", status.Convert(err).Message(), tt.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("OriginalURI() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("OriginalURI() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAcceptOrdering(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"grpcgateway-accept", "application/json",
		"accept", "text/csv",
		"accept", "application/vnd.sdmx.data+csv;version=2.0.0",
	))

	want := []string{"text/csv", "application/vnd.sdmx.data+csv;version=2.0.0", "application/json"}
	if got := Accept(ctx); !reflect.DeepEqual(got, want) {
		t.Fatalf("Accept() = %v, want %v", got, want)
	}
}

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

func TestNewRequest(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"x-envoy-original-path", "/sdmx/v3/data",
		"accept", "text/csv",
		util.XLogSDMX, "true",
	))

	got, err := NewRequest(ctx, "tail")
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	if got.Tail != "tail" {
		t.Fatalf("Tail = %q, want tail", got.Tail)
	}
	if got.OriginalURI != "/sdmx/v3/data" {
		t.Fatalf("OriginalURI = %q, want /sdmx/v3/data", got.OriginalURI)
	}
	if !reflect.DeepEqual(got.Accept, []string{"text/csv"}) {
		t.Fatalf("Accept = %v, want [text/csv]", got.Accept)
	}
	if !got.LogSDMX {
		t.Fatal("LogSDMX = false, want true")
	}
}

func TestNewRequestMissingURI(t *testing.T) {
	_, err := NewRequest(context.Background(), "tail")
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("NewRequest() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
	}
	if !strings.Contains(status.Convert(err).Message(), "missing SDMX request URI") {
		t.Fatalf("NewRequest() message = %q, want missing URI", status.Convert(err).Message())
	}
}
