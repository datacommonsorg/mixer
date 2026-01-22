package resolve

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestResolveUsingEmbeddings(t *testing.T) {
	// Mock the embeddings server response.
	// We simulate a response with two candidates to verify sorting and type detection logic.
	// Candidate 1: "Count_Person" (StatisticalVariable) with high score.
	// Candidate 2: "dc/topic/Population" (Topic) with lower score.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/search_vars" {
			t.Errorf("Expected path /api/search_vars, got %s", r.URL.Path)
		}

		if err := json.NewEncoder(w).Encode(map[string]interface{}{
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
		}); err != nil {
			t.Errorf("Failed to encode response: %v", err)
		}
	}))
	defer server.Close()

	ctx := context.Background()
	resp, err := ResolveUsingEmbeddings(ctx, server.Client(), server.URL, "test_idx", []string{"population"})
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

	// Verify that the score matches the server-provided CosineScore (0.99),
	// NOT the sentence score (0.95). This ensures we are using the primary ranking score.

	if candidate.Metadata["score"] != "0.9900" {
		t.Errorf("Expected score '0.9900', got '%s'", candidate.Metadata["score"])
	}

	if candidate.Metadata["sentence"] != "number of people" {
		t.Errorf("Expected sentence 'number of people', got '%s'", candidate.Metadata["sentence"])
	}

	// Verify the second candidate (Topic) to ensure TypeOf logic logic is working.

	if len(entity.Candidates) < 2 {
		t.Fatalf("Expected at least 2 candidates, got %d", len(entity.Candidates))
	}
	candidate2 := entity.Candidates[1]
	if candidate2.Dcid != "dc/topic/Population" {
		t.Errorf("Expected Dcid 'dc/topic/Population', got '%s'", candidate2.Dcid)
	}
	if len(candidate2.TypeOf) != 1 || candidate2.TypeOf[0] != "Topic" {
		t.Errorf("Expected TypeOf ['Topic'], got '%v'. This verifies Topic detection logic.", candidate2.TypeOf)
	}
	if candidate2.Metadata["score"] != "0.8800" {
		t.Errorf("Expected score '0.8800', got '%s'", candidate2.Metadata["score"])
	}
}

func TestResolveUsingEmbeddings_Errors(t *testing.T) {
	tests := []struct {
		name          string
		serverHandler http.HandlerFunc
		serverURL     string
		expectedError string
		useEmptyURL   bool
	}{
		{
			name: "Server Error",
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("internal error"))
			},
			expectedError: "The resolution service encountered an error processing your request.",
		},
		{
			name: "Malformed JSON",
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte("{invalid-json"))
			},
			expectedError: "An internal error occurred while parsing the resolution response.",
		},
		{
			name:          "Empty Server URL",
			useEmptyURL:   true,
			expectedError: "Indicator resolution is not available",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.serverHandler)
			defer server.Close()

			url := server.URL
			if tc.useEmptyURL {
				url = ""
			}

			_, err := ResolveUsingEmbeddings(context.Background(), server.Client(), url, "", []string{"query"})
			if err == nil {
				t.Errorf("Expected error containing '%s', got nil", tc.expectedError)
				return
			}

			if !strings.Contains(err.Error(), tc.expectedError) {
				t.Errorf("Expected error containing '%s', got '%v'", tc.expectedError, err)
			}
		})
	}
}

func TestResolveUsingEmbeddings_InconsistentSearchVarsResponse(t *testing.T) {
	// Mock a response with multiple inconsistencies:
	// 1. Array Mismatch: SV list (3 items) is longer than CosineScore list (2 items).
	// 2. Missing Metadata: "dcid2" has a score but NO entry in SV_to_Sentences.
	// Expected Behavior:
	// - Process "dcid1" (Normal)
	// - Process "dcid2" (Missing Sentence -> Score only)
	// - Ignore "dcid3" (No Score -> Safety Break)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewEncoder(w).Encode(map[string]interface{}{
			"queryResults": map[string]interface{}{
				"query": map[string]interface{}{
					"SV":          []string{"dcid1", "dcid2", "dcid3"}, // 3 SVs
					"CosineScore": []float64{0.99, 0.88},               // Only 2 Scores
					"SV_to_Sentences": map[string]interface{}{
						"dcid1": []interface{}{
							map[string]interface{}{"sentence": "s1", "score": 1.0},
						},
						// dcid2 is missing from map
					},
				},
			},
		}); err != nil {
			t.Errorf("Failed to encode response: %v", err)
		}
	}))
	defer server.Close()

	resp, err := ResolveUsingEmbeddings(context.Background(), server.Client(), server.URL, "", []string{"query"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(resp.Entities) != 1 {
		t.Fatalf("Expected 1 entity, got %d", len(resp.Entities))
	}
	entity := resp.Entities[0]

	// Should have 2 candidates (dcid1, dcid2). dcid3 is dropped.
	if len(entity.Candidates) != 2 {
		t.Fatalf("Expected 2 candidates, got %d", len(entity.Candidates))
	}

	// Verify Candidate 1 (Normal)
	c1 := entity.Candidates[0]
	if c1.Dcid != "dcid1" {
		t.Errorf("Candidate 1: Expected Dcid 'dcid1', got '%s'", c1.Dcid)
	}
	if val, ok := c1.Metadata["sentence"]; !ok || val != "s1" {
		t.Errorf("Candidate 1: Expected sentence 's1', got '%v'", val)
	}

	// Verify Candidate 2 (Missing Sentence)
	c2 := entity.Candidates[1]
	if c2.Dcid != "dcid2" {
		t.Errorf("Candidate 2: Expected Dcid 'dcid2', got '%s'", c2.Dcid)
	}
	if c2.Metadata["score"] != "0.8800" {
		t.Errorf("Candidate 2: Expected score '0.8800', got '%s'", c2.Metadata["score"])
	}
	if _, hasSentence := c2.Metadata["sentence"]; hasSentence {
		t.Errorf("Candidate 2: Expected NO sentence, but got one")
	}
}

func TestResolveUsingEmbeddings_IdxParameter(t *testing.T) {
	tests := []struct {
		name        string
		expectedIdx string
	}{
		{
			name:        "With Custom Indexes",
			expectedIdx: "custom_index_1,custom_index_2",
		},
		{
			name:        "With Empty Index",
			expectedIdx: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req searchVarsRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Errorf("Failed to decode request: %v", err)
					return
				}

				if req.Idx != tc.expectedIdx {
					t.Errorf("Expected idx '%s', got '%s'", tc.expectedIdx, req.Idx)
				}

				// Return empty valid response
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"queryResults": map[string]interface{}{},
				})
			}))
			defer server.Close()

			_, err := ResolveUsingEmbeddings(context.Background(), server.Client(), server.URL, tc.expectedIdx, []string{"query"})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}
