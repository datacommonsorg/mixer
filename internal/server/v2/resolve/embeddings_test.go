package resolve

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/datacommonsorg/mixer/internal/config"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var testLabelToIndex = map[string]string{
	"multi-entity": "base_multi_entity",
	"base-nl":      "base_uae_mem",
}

func TestResolveUsingEmbeddings(t *testing.T) {
	// Mock the embeddings server response.
	// We simulate a response with two candidates to verify sorting and type detection logic.
	// Candidate 1: "Count_Person" (StatisticalVariable) with high score.
	// Candidate 2: "dc/topic/Population" (Topic) with lower score.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != SearchVarsQueryEndpoint {
			t.Errorf("Expected path %s, got %s", SearchVarsQueryEndpoint, r.URL.Path)
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
	cfg := &config.ParsedEmbeddingsConfig{URL: server.URL}
	client := NewEmbeddingsServiceClient(server.Client(), cfg)
	resp, err := client.Resolve(ctx, "test_idx", []string{"population"}, nil, nil, false)
	if err != nil {
		t.Fatalf("Resolve() error: %v", err)
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
		expectedCode  codes.Code
		useEmptyURL   bool
		useNilConfig  bool
	}{
		{
			name: "Server Error (500)",
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("internal error"))
			},
			expectedError: "internal error",
			expectedCode:  codes.Internal,
		},
		{
			name: "Bad Request (400)",
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte("bad index name"))
			},
			expectedError: "bad index name",
			expectedCode:  codes.InvalidArgument,
		},
		{
			name: "Not Found (404)",
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte("index not found"))
			},
			expectedError: "index not found",
			expectedCode:  codes.NotFound,
		},
		{
			name: "Malformed JSON",
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte("{invalid-json"))
			},
			expectedError: "An internal error occurred while parsing the resolution response.",
			expectedCode:  codes.Internal,
		},
		{
			name:          "Empty Server URL",
			useEmptyURL:   true,
			expectedError: "Indicator resolution is not available",
			expectedCode:  codes.FailedPrecondition,
		},
		{
			name:          "Nil Config",
			useNilConfig:  true,
			expectedError: "Embeddings configuration is not loaded",
			expectedCode:  codes.FailedPrecondition,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var server *httptest.Server
			var url string
			if !tc.useEmptyURL {
				server = httptest.NewServer(tc.serverHandler)
				defer server.Close()
				url = server.URL
			}

			var client *http.Client
			if server != nil {
				client = server.Client()
			} else {
				client = http.DefaultClient
			}
			var cfg *config.ParsedEmbeddingsConfig
			if !tc.useNilConfig {
				cfg = &config.ParsedEmbeddingsConfig{URL: url}
			}
			embeddingsServiceClient := NewEmbeddingsServiceClient(client, cfg)
			_, err := embeddingsServiceClient.Resolve(context.Background(), "", []string{"query"}, nil, nil, false)
			if err == nil {
				t.Errorf("Expected error containing '%s', got nil", tc.expectedError)
				return
			}

			if !strings.Contains(err.Error(), tc.expectedError) {
				t.Errorf("Expected error containing '%s', got '%v'", tc.expectedError, err)
			}

			status, ok := status.FromError(err)
			if !ok {
				t.Fatalf("Expected gRPC status error, got %v", err)
			}
			if status.Code() != tc.expectedCode {
				t.Errorf("Expected error code %v, got %v", tc.expectedCode, status.Code())
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

	cfg := &config.ParsedEmbeddingsConfig{URL: server.URL}
	client := NewEmbeddingsServiceClient(server.Client(), cfg)
	resp, err := client.Resolve(context.Background(), "", []string{"query"}, nil, nil, false)
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
				// Verify idx is in the URL query parameters
				gotIdx := r.URL.Query().Get("idx")
				if gotIdx != tc.expectedIdx {
					t.Errorf("Expected idx '%s' in URL, got '%s'", tc.expectedIdx, gotIdx)
				}

				var req searchVarsRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Errorf("Failed to decode request: %v", err)
					return
				}

				// Return empty valid response
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"queryResults": map[string]interface{}{},
				})
			}))
			defer server.Close()

			cfg := &config.ParsedEmbeddingsConfig{URL: server.URL}
			client := NewEmbeddingsServiceClient(server.Client(), cfg)
			_, err := client.Resolve(context.Background(), tc.expectedIdx, []string{"query"}, nil, nil, false)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

func TestResolveUsingEmbeddings_Filter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewEncoder(w).Encode(map[string]interface{}{
			"queryResults": map[string]interface{}{
				"filter_test": map[string]interface{}{
					"SV":              []string{"Count_Person", "dc/topic/Population"},
					"CosineScore":     []float64{0.99, 0.88},
					"SV_to_Sentences": map[string]interface{}{},
				},
			},
		}); err != nil {
			t.Errorf("Failed to encode response: %v", err)
		}
	}))
	defer server.Close()

	ctx := context.Background()

	cfg := &config.ParsedEmbeddingsConfig{URL: server.URL}
	client := NewEmbeddingsServiceClient(server.Client(), cfg)

	// Filter for StatisticalVariable
	resp, err := client.Resolve(ctx, "test_idx", []string{"filter_test"}, []string{"StatisticalVariable"}, nil, false)
	if err != nil {
		t.Fatalf("Resolve() error: %v", err)
	}
	if len(resp.Entities[0].Candidates) != 1 {
		t.Fatalf("Expected 1 candidate, got %d", len(resp.Entities[0].Candidates))
	}
	if resp.Entities[0].Candidates[0].Dcid != "Count_Person" {
		t.Errorf("Expected 'Count_Person', got '%s'", resp.Entities[0].Candidates[0].Dcid)
	}

	// Filter for Topic
	resp, err = client.Resolve(ctx, "test_idx", []string{"filter_test"}, []string{"Topic"}, nil, false)
	if err != nil {
		t.Fatalf("Resolve() error: %v", err)
	}
	if len(resp.Entities[0].Candidates) != 1 {
		t.Fatalf("Expected 1 candidate, got %d", len(resp.Entities[0].Candidates))
	}
	if resp.Entities[0].Candidates[0].Dcid != "dc/topic/Population" {
		t.Errorf("Expected 'dc/topic/Population', got '%s'", resp.Entities[0].Candidates[0].Dcid)
	}
}

func TestSelectEmbeddingsIndex(t *testing.T) {
	tests := []struct {
		name               string
		headerValue        string
		expectedIdx        string
		expectError        bool
		expectedErrorCode  codes.Code
		passNilConfig      bool
	}{
		{
			// Test: Fallback to default index.
			// Situation: No X-V2Resolve-Index header is provided.
			// Expectation: Returns the defaultIndex.
			name:               "No header",
			headerValue:        "",
			expectedIdx:        "default_idx",
			expectError:        false,
		},
		{
			// Test: Successful override to multi-entity index.
			// Situation: Header is set to "multi-entity".
			// Expectation: Returns "base_multi_entity".
			name:               "Valid header, enabled",
			headerValue:        "multi-entity",
			expectedIdx:        "base_multi_entity",
			expectError:        false,
		},
		{
			// Test: Successful override to base-nl index.
			// Situation: Header is set to "base-nl".
			// Expectation: Returns "base_uae_mem".
			name:               "Valid header (base-nl), enabled",
			headerValue:        "base-nl",
			expectedIdx:        "base_uae_mem",
			expectError:        false,
		},
		{
			// Test: Pass through unknown label.
			// Situation: Header is set to an unknown value "invalid".
			// Expectation: Returns "invalid".
			name:               "Invalid header, pass through",
			headerValue:        "invalid",
			expectedIdx:        "invalid",
			expectError:        false,
		},
		{
			name:               "Nil config",
			headerValue:        "",
			expectError:        true,
			expectedErrorCode:  codes.FailedPrecondition,
			passNilConfig:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.headerValue != "" {
				md := metadata.Pairs(util.XV2ResolveIndex, tc.headerValue)
				ctx = metadata.NewIncomingContext(ctx, md)
			}

			var cfg *config.ParsedEmbeddingsConfig
			if !tc.passNilConfig {
				cfg = &config.ParsedEmbeddingsConfig{
					DefaultIndex:        "default_idx",
					ResolveIndexMapping: testLabelToIndex,
				}
			}
			client := NewEmbeddingsServiceClient(nil, cfg)
			idx, err := client.SelectIndex(ctx)

			if tc.expectError {
				if err == nil {
					t.Fatalf("Expected error, got nil")
				}
				status, ok := status.FromError(err)
				if !ok {
					t.Fatalf("Expected gRPC status error, got %v", err)
				}
				if status.Code() != tc.expectedErrorCode {
					t.Errorf("Expected error code %v, got %v", tc.expectedErrorCode, status.Code())
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if idx != tc.expectedIdx {
					t.Errorf("Expected index '%s', got '%s'", tc.expectedIdx, idx)
				}
			}
		})
	}
}



