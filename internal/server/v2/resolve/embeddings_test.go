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

package resolve

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestResolveEmbeddings(t *testing.T) {
	// Mock the sidecar response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/search_vars" {
			t.Errorf("Expected path /api/search_vars, got %s", r.URL.Path)
		}

		json.NewEncoder(w).Encode(map[string]interface{}{
			"queryResults": map[string]interface{}{
				"population": map[string]interface{}{
					"SV":          []string{"Count_Person", "dc/topic/Population"},
					"CosineScore": []float64{0.99, 0.88},
					"SV_to_Sentences": map[string]interface{}{
						"Count_Person": []interface{}{
							map[string]interface{}{
								"sentence": "number of people",
								"score":    0.95,
							},
						},
					},
				},
			},
		})
	}))
	defer server.Close()

	ctx := context.Background()
	resp, err := ResolveUsingEmbeddings(ctx, server.Client(), server.URL, []string{"population"})
	if err != nil {
		t.Fatalf("ResolveEmbeddings() error: %v", err)
	}

	if len(resp.Entities) != 1 {
		t.Fatalf("Expected 1 entity, got %d", len(resp.Entities))
	}

	entity := resp.Entities[0]
	if entity.Node != "population" {
		t.Errorf("Expected node 'population', got '%s'", entity.Node)
	}

	if len(entity.Candidates) != 2 {
		t.Fatalf("Expected 2 candidates, got %d", len(entity.Candidates))
	}

	candidate := entity.Candidates[0]
	if candidate.Dcid != "Count_Person" {
		t.Errorf("Expected Dcid 'Count_Person', got '%s'", candidate.Dcid)
	}

	if candidate.Metadata["score"] != "0.9900" {
		t.Errorf("Expected score '0.9900', got '%s'", candidate.Metadata["score"])
	}

	if candidate.Metadata["sentence"] != "number of people" {
		t.Errorf("Expected sentence 'number of people', got '%s'", candidate.Metadata["sentence"])
	}

	if len(entity.Candidates) < 2 {
		t.Fatalf("Expected at least 2 candidates, got %d", len(entity.Candidates))
	}
	candidate2 := entity.Candidates[1]
	if candidate2.Dcid != "dc/topic/Population" {
		t.Errorf("Expected Dcid 'dc/topic/Population', got '%s'", candidate2.Dcid)
	}
	if len(candidate2.TypeOf) != 1 || candidate2.TypeOf[0] != "Topic" {
		t.Errorf("Expected TypeOf ['Topic'], got '%v'", candidate2.TypeOf)
	}
	if candidate2.Metadata["score"] != "0.8800" {
		t.Errorf("Expected score '0.8800', got '%s'", candidate2.Metadata["score"])
	}
}
