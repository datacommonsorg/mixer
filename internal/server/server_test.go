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

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/datacommonsorg/mixer/internal/featureflags"
	"github.com/datacommonsorg/mixer/internal/server/v2/resolve"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/metadata"
)

type testNLServerConfig struct {
	Indexes map[string]interface{} `json:"indexes"`
}

func TestValidateEmbeddingsIndex(t *testing.T) {
	tests := []struct {
		name          string
		serverURL     string
		serverHandler http.HandlerFunc
		idxToValidate string
		wantValid     bool
	}{
		{
			name:          "Empty Server URL (lenient)",
			serverURL:     "",
			idxToValidate: "any_index",
			wantValid:     true,
		},
		{
			name: "Valid Index",
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(testNLServerConfig{
					Indexes: map[string]interface{}{
						"index1": map[string]interface{}{},
						"index2": map[string]interface{}{},
					},
				})
			},
			idxToValidate: "index1",
			wantValid:     true,
		},
		{
			name: "Invalid Index",
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(testNLServerConfig{
					Indexes: map[string]interface{}{
						"index1": map[string]interface{}{},
					},
				})
			},
			idxToValidate: "index2",
			wantValid:     false,
		},
		{
			name: "Multiple Valid Indexes",
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(testNLServerConfig{
					Indexes: map[string]interface{}{
						"index1": map[string]interface{}{},
						"index2": map[string]interface{}{},
					},
				})
			},
			idxToValidate: "index1, index2",
			wantValid:     true,
		},
		{
			name: "Multiple Indexes - One Invalid",
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(testNLServerConfig{
					Indexes: map[string]interface{}{
						"index1": map[string]interface{}{},
					},
				})
			},
			idxToValidate: "index1, index2",
			wantValid:     false,
		},
		{
			name: "Server Error (lenient fallback)",
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			idxToValidate: "any_index",
			wantValid:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var serverURL string
			var testServer *httptest.Server
			if tc.serverHandler != nil {
				testServer = httptest.NewServer(tc.serverHandler)
				defer testServer.Close()
				serverURL = testServer.URL
			} else {
				serverURL = tc.serverURL
			}

			client := resolve.NewEmbeddingsServiceClient(&http.Client{}, serverURL, "")
			_ = client.LoadAvailableIndexes(context.Background())
			got := client.ValidateIndex(context.Background(), tc.idxToValidate)
			if got != tc.wantValid {
				t.Errorf("ValidateIndex() = %v, want %v", got, tc.wantValid)
			}
		})
	}
}

func TestShouldDivertV2(t *testing.T) {

	tests := []struct {
		name            string
		useSpannerGraph bool
		flagUseSpanner  bool
		divertFraction  float64
		headerValue     string // Use "" to simulate a missing header
		expected        bool
	}{
		{
			name:            "Branch 1: Overall useSpannerGraph flag true forces diversion",
			useSpannerGraph: true,
			flagUseSpanner:  false,   // Should be ignored
			headerValue:     "false", // Should be ignored
			expected:        true,
		},
		{
			name:            "Branch 2: Experimental UseSpannerGraph flag false prevents diversion",
			useSpannerGraph: false,
			flagUseSpanner:  false,
			headerValue:     "true", // Should be ignored
			expected:        false,
		},
		{
			name:            "Branch 3: Header is explicitly true",
			useSpannerGraph: false,
			flagUseSpanner:  true,
			headerValue:     "true",
			expected:        true,
		},
		{
			name:            "Branch 4: Header is false, fraction is 0, returns false",
			useSpannerGraph: false,
			flagUseSpanner:  true,
			headerValue:     "false",
			divertFraction:  0.0,
			expected:        false,
		},
		{
			name:            "Branch 5: No header, fraction is 1.0 (100% diversion)",
			useSpannerGraph: false,
			flagUseSpanner:  true,
			headerValue:     "",
			divertFraction:  1.0, // rand.Float64() is always < 1.0
			expected:        true,
		},
		{
			name:            "Branch 6: Missing header, negative fraction is safely ignored",
			useSpannerGraph: false,
			flagUseSpanner:  true,
			headerValue:     "",
			divertFraction:  -0.5,
			expected:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.headerValue != "" {
				md := metadata.Pairs(util.XDivertSpanner, tt.headerValue)
				ctx = metadata.NewIncomingContext(ctx, md)
			}

			s := &Server{
				useSpannerGraph: tt.useSpannerGraph,
				flags: &featureflags.Flags{
					UseSpannerGraph:  tt.flagUseSpanner,
					V2DivertFraction: tt.divertFraction,
				},
			}

			got := s.shouldDivertV2(ctx)
			if got != tt.expected {
				t.Errorf("shouldDivertV2() = %v, want %v", got, tt.expected)
			}
		})
	}
}
