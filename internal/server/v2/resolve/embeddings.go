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

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	embeddingsURL = "http://localhost:6060/api/search_vars"
)

// EmbeddingsRequest represents the request body for the sidecar
type EmbeddingsRequest struct {
	Queries []string `json:"queries"`
}

// EmbeddingsResponse represents the response body from the sidecar
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

// ResolveEmbeddings calls the sidecar to resolve natural language queries to SVs.
func ResolveEmbeddings(
	ctx context.Context,
	httpClient *http.Client,
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
	req, err := http.NewRequestWithContext(ctx, "POST", embeddingsURL, bytes.NewBuffer(jsonBody))
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
			for i, fileID := range result.SV {
				// Safety check for parallel arrays
				if i >= len(result.CosineScore) {
					break
				}

				score := result.CosineScore[i]
				candidate := &pbv2.ResolveResponse_Entity_Candidate{
					Dcid:         fileID,
					DominantType: "StatisticalVariable",
					Metadata: map[string]string{
						"score": fmt.Sprintf("%f", score),
					},
				}

				// Add sentence metadata if available
				if sentences, hasSentences := result.SVToSentences[fileID]; hasSentences && len(sentences) > 0 {
					// Taking the top sentence match
					candidate.Metadata["sentence"] = sentences[0].Sentence
					candidate.Metadata["sentence_score"] = fmt.Sprintf("%f", sentences[0].Score)
				}

				candidates = append(candidates, candidate)
			}
			entity.Candidates = candidates
		}
		pbResp.Entities = append(pbResp.Entities, entity)
	}

	return pbResp, nil
}
