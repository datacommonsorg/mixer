// Copyright 2024 Google LLC
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

// EmbeddingsRequest represents the request body for the embeddings server
type EmbeddingsRequest struct {
	Queries []string `json:"queries"`
}

// EmbeddingsResponse represents the response body from the embeddings server
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
type EmbeddingsResponse struct {
	QueryResults map[string]struct {
		SV            []string  `json:"SV"`
		CosineScore   []float64 `json:"CosineScore"`
		SVToSentences map[string][]struct {
			Sentence string  `json:"sentence"`
			Score    float64 `json:"score"`
		} `json:"SV_to_Sentences"`
	} `json:"queryResults"`
}

// ResolveUsingEmbeddings calls the embeddings server to resolve natural language queries to SVs.
func ResolveUsingEmbeddings(
	ctx context.Context,
	httpClient *http.Client,
	embeddingsServerURL string,
	nodes []string,
) (*pbv2.ResolveResponse, error) {
	// Construct the request body
	reqBody := EmbeddingsRequest{
		Queries: nodes,
	}
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal embeddings request: %v", err)
	}

	// Create the HTTP request
	req, err := http.NewRequestWithContext(ctx, "POST", embeddingsServerURL+"/api/search_vars", bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create embeddings request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Execute the request
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "failed to contact embeddings sidecar: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, status.Errorf(codes.Internal, "embeddings sidecar returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	// Parse the response
	var embResp EmbeddingsResponse
	if err := json.NewDecoder(resp.Body).Decode(&embResp); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to decode embeddings response: %v", err)
	}

	// Map to protobuf response
	pbResp := &pbv2.ResolveResponse{
		Entities: make([]*pbv2.ResolveResponse_Entity, 0, len(nodes)),
	}

	for _, node := range nodes {
		entity := &pbv2.ResolveResponse_Entity{
			Node: node,
		}

		if result, ok := embResp.QueryResults[node]; ok {
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

				candidate := &pbv2.ResolveResponse_Entity_Candidate{
					Dcid:   statVarDcid,
					TypeOf: []string{dominantType},
					Metadata: map[string]string{
						"score": fmt.Sprintf("%f", score),
					},
				}

				if sentences, hasSentences := result.SVToSentences[statVarDcid]; hasSentences && len(sentences) > 0 {
					// Taking the top sentence match for display purposes
					candidate.Metadata["sentence"] = sentences[0].Sentence
				}

				candidates = append(candidates, candidate)
			}
			entity.Candidates = candidates
		}
		pbResp.Entities = append(pbResp.Entities, entity)
	}

	return pbResp, nil
}
