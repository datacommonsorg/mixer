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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	TopicDcidSubstring              = "/topic/"
	TopicDominantType               = "Topic"
	StatisticalVariableDominantType = "StatisticalVariable"
)

// searchVarsRequest represents the request body for the embeddings server
type searchVarsRequest struct {
	Queries []string `json:"queries"`
}

// searchVarsResponse represents the response body from the embeddings server
//
// Expected JSON Structure:
//
//	{
//	  "queryResults": {
//	    "your_query_string": {
//	      "SV": [ "dcid1", "dcid2", ... ],            // Sorted list of SV DCIDs
//	      "CosineScore": [ 0.9, 0.8, ... ],           // Corresponding overall scores (parallel to SV list)
//	      "SV_to_Sentences": {                        // Map of SV DCID to matching sentences
//	        "dcid1": [
//	          { "sentence": "description text", "score": 0.95 },
//	          ...
//	        ]
//	      }
//	    }
//	  }
//	}
type searchVarsResponse struct {
	QueryResults map[string]searchResult `json:"queryResults"`
}

type searchResult struct {
	SV            []string  `json:"SV"`
	CosineScore   []float64 `json:"CosineScore"`
	SVToSentences map[string][]struct {
		Sentence string  `json:"sentence"`
		Score    float64 `json:"score"`
	} `json:"SV_to_Sentences"`
}

// ResolveUsingEmbeddings calls the embeddings server to resolve natural language queries to SVs.
//
// It performs the following steps:
// 1. Validates the embeddings server configuration.
// 2. Calls the embeddings server via HTTP to get search results (callEmbeddingsServer).
// 3. Transforms the raw search results into the resolved response format (buildResolveResponse).
func ResolveUsingEmbeddings(
	ctx context.Context,
	httpClient *http.Client,
	embeddingsServerURL string,
	idx string,
	nodes []string,
	typeOfValues []string,
) (*pbv2.ResolveResponse, error) {
	if embeddingsServerURL == "" {
		slog.Error("resolver=indicator requested, but the embeddings server is not configured for this deployment")
		return nil, status.Errorf(codes.FailedPrecondition, "Indicator resolution is not available in this environment.")
	}

	searchResp, err := callEmbeddingsServer(ctx, httpClient, embeddingsServerURL, idx, nodes)
	if err != nil {
		return nil, err
	}

	return buildResolveResponse(nodes, searchResp, typeOfValues), nil
}

// callEmbeddingsServer handles the HTTP communication with the embeddings server.
//
// Inputs:
//   - ctx: Context for the request.
//   - httpClient: HTTP client to use for the request.
//   - embeddingsServerURL: Base URL of the embeddings server.
//   - idx: The index to use for resolution.
//   - nodes: List of query strings to resolve.
//
// Returns:
//   - *searchVarsResponse: The parsed JSON response from the server.
//   - error: Any error encountered during the request or parsing.
func callEmbeddingsServer(
	ctx context.Context,
	httpClient *http.Client,
	embeddingsServerURL string,
	idx string,
	nodes []string,
) (*searchVarsResponse, error) {
	// Construct the request body
	searchReq := searchVarsRequest{
		Queries: nodes,
	}
	requestBytes, err := json.Marshal(searchReq)
	if err != nil {
		slog.Error("Internal error preparing query for resolution", "error", err, "query_count", len(nodes))
		return nil, status.Errorf(codes.Internal, "Failed to encode the query parameters for resolution.")
	}

	// Create the HTTP request
	url := embeddingsServerURL + "/api/search_vars"
	// The embeddings server expects the index to be passed as a query parameter
	if idx != "" {
		url += "?idx=" + idx
	}
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(requestBytes))
	if err != nil {
		slog.Error("Failed to create embeddings server request", "error", err, "url", embeddingsServerURL)
		return nil, status.Errorf(codes.Internal, "An internal error occurred while connecting to the resolution service.")
	}
	req.Header.Set("Content-Type", "application/json")

	// Execute the request
	httpResp, err := httpClient.Do(req)
	if err != nil {
		slog.Error("Failed to contact embeddings server", "error", err, "url", embeddingsServerURL, "queries", nodes)
		return nil, status.Errorf(codes.Unavailable, "The resolution service is currently unavailable. Please try again later.")
	}
	defer func() {
		// Drain and close the body to let the Transport reuse the connection
		_, _ = io.Copy(io.Discard, httpResp.Body)
		_ = httpResp.Body.Close()
	}()

	if httpResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(httpResp.Body)
		slog.Error("Embeddings server returned non-200 status", "status_code", httpResp.StatusCode, "body", string(bodyBytes), "url", embeddingsServerURL, "queries", nodes)
		return nil, status.Errorf(codes.Internal, "The resolution service encountered an error processing your request.")
	}

	// Parse the response
	var searchResp searchVarsResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&searchResp); err != nil {
		slog.Error("Failed to decode embeddings server response", "error", err, "url", embeddingsServerURL)
		return nil, status.Errorf(codes.Internal, "An internal error occurred while parsing the resolution response.")
	}
	return &searchResp, nil
}

// buildResolveResponse converts the raw search response into the gRPC ResolveResponse.
//
// It iterates through the original requested nodes (queries) and attempts to find
// corresponding results in the searchResp. If results are found, it delegates
// to buildEntityCandidates to construct the candidate list.
func buildResolveResponse(nodes []string, searchResp *searchVarsResponse, typeOfValues []string) *pbv2.ResolveResponse {
	resolveResponse := &pbv2.ResolveResponse{
		Entities: make([]*pbv2.ResolveResponse_Entity, 0, len(nodes)),
	}

	for _, node := range nodes {
		entity := &pbv2.ResolveResponse_Entity{
			Node: node,
		}

		if result, ok := searchResp.QueryResults[node]; ok {
			entity.Candidates = buildEntityCandidates(&result, typeOfValues)
		}
		resolveResponse.Entities = append(resolveResponse.Entities, entity)
	}
	return resolveResponse
}

// buildEntityCandidates constructs a list of ResolveResponse_Entity_Candidate from a single searchResult.
//
// It maps the server's response format (SV list + scores) into the mixer's candidate format.
// Key logic included:
//   - Uses 'CosineScore' as the primary score.
//   - Determines 'DominantType' based on the DCID (Topic vs StatVar).
//   - Extracts the best matching sentence for metadata if available.
func buildEntityCandidates(result *searchResult, typeOfValues []string) []*pbv2.ResolveResponse_Entity_Candidate {
	candidates := make([]*pbv2.ResolveResponse_Entity_Candidate, 0, len(result.SV))
	for i, statVarDcid := range result.SV {
		// Safety check for parallel arrays
		if i >= len(result.CosineScore) {
			break
		}

		// Key logic: We use the server-provided CosineScore as the primary match score.
		// This score is typically derived from the best matching sentence on the server side.
		score := result.CosineScore[i]

		dominantType := StatisticalVariableDominantType
		if strings.Contains(statVarDcid, TopicDcidSubstring) {
			dominantType = TopicDominantType
		}

		// Filter by type if provided
		if len(typeOfValues) > 0 {
			match := false
			for _, t := range typeOfValues {
				if t == dominantType {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}

		candidate := &pbv2.ResolveResponse_Entity_Candidate{
			Dcid:   statVarDcid,
			TypeOf: []string{dominantType},
			Metadata: map[string]string{
				"score": fmt.Sprintf("%.4f", score),
			},
		}

		if sentences, hasSentences := result.SVToSentences[statVarDcid]; hasSentences && len(sentences) > 0 {
			// Taking the top sentence match for display purposes
			candidate.Metadata["sentence"] = sentences[0].Sentence
		}

		candidates = append(candidates, candidate)
	}
	return candidates
}
