package resolve

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

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
	client := NewEmbeddingsServiceClient(server.Client(), server.URL, "")
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
	}{
		{
			name: "Server Error (500)",
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("internal error"))
			},
			expectedError: "The embeddings service backend encountered an error processing your request.",
			expectedCode:  codes.Internal,
		},
		{
			name: "Bad Request (400)",
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte("bad index name"))
			},
			expectedError: "The embeddings service backend encountered an error processing your request.",
			expectedCode:  codes.InvalidArgument,
		},
		{
			name: "Not Found (404)",
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte("index not found"))
			},
			expectedError: "The embeddings service backend encountered an error processing your request.",
			expectedCode:  codes.NotFound,
		},
		{
			name: "Malformed JSON",
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte("{invalid-json"))
			},
			expectedError: "An internal error occurred while parsing the response from the embeddings service backend.",
			expectedCode:  codes.Internal,
		},
		{
			name:          "Empty Server URL",
			useEmptyURL:   true,
			expectedError: "Indicator resolution is not available",
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

			embeddingsServiceClient := NewEmbeddingsServiceClient(client, url, "")
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

	client := NewEmbeddingsServiceClient(server.Client(), server.URL, "")
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

			client := NewEmbeddingsServiceClient(server.Client(), server.URL, "")
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

	client := NewEmbeddingsServiceClient(server.Client(), server.URL, "")

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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.headerValue != "" {
				md := metadata.Pairs(util.XV2ResolveIndex, tc.headerValue)
				ctx = metadata.NewIncomingContext(ctx, md)
			}

			client := NewEmbeddingsServiceClient(nil, "", "default_idx")
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

func TestValidateIndex_OnDemandLoad(t *testing.T) {
	var requestCount int
	var lock sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/server_config" {
			t.Errorf("Unexpected path: %s", r.URL.Path)
		}

		lock.Lock()
		requestCount++
		lock.Unlock()

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"indexes": map[string]interface{}{
				"base_uae_mem": map[string]interface{}{},
			},
		})
	}))
	defer server.Close()

	client := NewEmbeddingsServiceClient(server.Client(), server.URL, "")

	ctx := context.Background()

	// 1. First call with valid index. This should block and fetch config, returning true.
	isValid := client.ValidateIndex(ctx, "base_uae_mem")
	if !isValid {
		t.Errorf("Expected ValidateIndex to return true for valid index")
	}

	// 2. Verify exactly 1 request was made to the server.
	lock.Lock()
	count := requestCount
	lock.Unlock()
	if count != 1 {
		t.Errorf("Expected 1 request to embeddings server, got %d", count)
	}

	// 3. Second call with invalid index. It should use cached config and return false immediately.
	isValid = client.ValidateIndex(ctx, "invalid_index")
	if isValid {
		t.Errorf("Expected ValidateIndex to return false for invalid index")
	}

	// 4. Verify no new requests were made.
	lock.Lock()
	count = requestCount
	lock.Unlock()
	if count != 1 {
		t.Errorf("Expected still only 1 request to embeddings server, got %d", count)
	}
}

// Test: Constructor with nil HTTP Client.
// Situation: We initialize the client passing nil for the httpClient parameter.
// Expectation: The client is initialized with a default HTTP client, and calling methods on it does not panic.
func TestNewEmbeddingsServiceClient_NilClient(t *testing.T) {
	var requestCount int
	var lock sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		requestCount++
		lock.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
	}))
	defer server.Close()

	// Pass nil for httpClient
	client := NewEmbeddingsServiceClient(nil, server.URL, "")
	if client.httpClient == nil {
		t.Fatal("Expected httpClient to be initialized to default, got nil")
	}

	ctx := context.Background()
	// Trigger index validation which executes a GET request using the client
	// It should block and make 1 request.
	_ = client.ValidateIndex(ctx, "any_index")

	lock.Lock()
	count := requestCount
	lock.Unlock()
	if count != 1 {
		t.Errorf("Expected 1 request to embeddings server, got %d", count)
	}
}

// Test: Concurrent on-demand loading.
// Situation: 50 concurrent requests try to validate indexes when they are not loaded yet.
// Expectation: Only one HTTP call is made to the embeddings server (meaning only one goroutine is spawned),
//              and all requests return true (leniently).
func TestValidateIndex_ConcurrentOnDemandLoad(t *testing.T) {
	var requestCount int
	var lock sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		requestCount++
		lock.Unlock()

		// Add artificial delay to simulate slow HTTP fetch and let other concurrent requests block
		time.Sleep(50 * time.Millisecond)

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"indexes": {"base_uae_mem": {}}}`))
	}))
	defer server.Close()

	client := NewEmbeddingsServiceClient(&http.Client{}, server.URL, "")

	ctx := context.Background()
	var wg sync.WaitGroup
	numConcurrentRequests := 50
	results := make([]bool, numConcurrentRequests)

	for i := 0; i < numConcurrentRequests; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			results[index] = client.ValidateIndex(ctx, "base_uae_mem")
		}(i)
	}

	wg.Wait()

	// 1. All concurrent requests should return true because the index is valid
	for i, res := range results {
		if !res {
			t.Errorf("Request %d: Expected true (valid index) after sync config load", i)
		}
	}

	// 2. Verify exactly 1 request was made to the backend
	lock.Lock()
	count := requestCount
	lock.Unlock()

	if count != 1 {
		t.Errorf("Expected exactly 1 request to embeddings server, got %d (redundant fetches were probably made)", count)
	}
}
