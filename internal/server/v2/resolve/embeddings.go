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
	"net/url"
	"path"
	"strings"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	TopicDcidSubstring              = "/topic/"
	TopicDominantType               = "Topic"
	StatisticalVariableDominantType = "StatisticalVariable"
	SearchVarsQueryEndpoint         = "/api/search_vars"
)

const (
	LabelMultiEntity     = "multi-entity"
	IndexBaseMultiEntity = "base_multi_entity"
	LabelBaseNL          = "base-nl"
	IndexBaseUaeMem      = "base_uae_mem"
)

var labelToIndex = map[string]string{
	LabelMultiEntity: IndexBaseMultiEntity,
	LabelBaseNL:      IndexBaseUaeMem,
}

type EmbeddingsServiceClientOptions struct {
	HTTPClient     *http.Client
	DefaultIndexes string
}

type EmbeddingsServiceClient struct {
	httpClient          *http.Client
	embeddingsServerURL string
	defaultIndexes      string
}

func NewEmbeddingsServiceClient(serverURL string, opts *EmbeddingsServiceClientOptions) *EmbeddingsServiceClient {
	client := &EmbeddingsServiceClient{
		httpClient:          &http.Client{},
		embeddingsServerURL: serverURL,
	}
	if opts != nil {
		if opts.HTTPClient != nil {
			client.httpClient = opts.HTTPClient
		}
		client.defaultIndexes = opts.DefaultIndexes
	}
	return client
}

// SelectIndex determines the correct index to use for embeddings resolution.
func (c *EmbeddingsServiceClient) SelectIndex(ctx context.Context) (string, error) {
	label := util.GetSingleHeaderValue(ctx, util.XV2ResolveIndex)
	if label == "" {
		return c.defaultIndexes, nil
	}

	if indexName, ok := labelToIndex[label]; ok {
		return indexName, nil
	}

	// If not in the map, just pass the label directly.
	slog.Info("V2Resolve index label not in map, passing directly", "label", label, "header", util.XV2ResolveIndex)
	return label, nil
}

// searchVarsRequest represents the request body for the embeddings server
type searchVarsRequest struct {
	Queries []string `json:"queries"`
}

// searchVarsResponse represents the response body from the embeddings server
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

// Resolve calls the embeddings server to resolve natural language queries to SVs.
func (c *EmbeddingsServiceClient) Resolve(
	ctx context.Context,
	idx string,
	nodes []string,
	typeOfValues []string,
	topicExpander TopicExpander,
	expandTopics bool,
) (*pbv2.ResolveResponse, error) {
	if c.embeddingsServerURL == "" {
		slog.Error("resolver=indicator requested, but the embeddings server is not configured for this deployment")
		return nil, status.Errorf(codes.FailedPrecondition, "Indicator resolution is not available in this environment.")
	}

	searchResp, err := c.callEmbeddingsServer(ctx, idx, nodes)
	if err != nil {
		return nil, err
	}

	return buildResolveResponse(ctx, nodes, searchResp, typeOfValues, topicExpander, expandTopics), nil
}

// callEmbeddingsServer handles the HTTP communication with the embeddings server.
func (c *EmbeddingsServiceClient) callEmbeddingsServer(
	ctx context.Context,
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
	searchVarsURL, err := url.Parse(c.embeddingsServerURL)
	if err != nil {
		slog.Error("Failed to parse embeddings server URL", "error", err, "url", c.embeddingsServerURL)
		return nil, status.Errorf(codes.Internal, "An internal error occurred while connecting to the resolution service.")
	}
	searchVarsURL.Path = path.Join(searchVarsURL.Path, SearchVarsQueryEndpoint)

	// The embeddings server expects the index to be passed as a query parameter
	if idx != "" {
		q := searchVarsURL.Query()
		q.Set("idx", idx)
		searchVarsURL.RawQuery = q.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, "POST", searchVarsURL.String(), bytes.NewBuffer(requestBytes))
	if err != nil {
		slog.Error("Failed to create embeddings server request", "error", err, "url", c.embeddingsServerURL)
		return nil, status.Errorf(codes.Internal, "An internal error occurred while connecting to the resolution service.")
	}
	req.Header.Set("Content-Type", "application/json")

	// Execute the request
	httpResp, err := c.httpClient.Do(req)
	if err != nil {
		slog.Error("Failed to contact embeddings server", "error", err, "url", c.embeddingsServerURL, "queries", nodes)
		return nil, status.Errorf(codes.Unavailable, "The resolution service is currently unavailable. Please try again later.")
	}
	defer func() {
		// Drain and close the body to let the Transport reuse the connection
		_, _ = io.Copy(io.Discard, httpResp.Body)
		_ = httpResp.Body.Close()
	}()

	if httpResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(httpResp.Body)
		slog.Error("Embeddings server returned non-200 status", "status_code", httpResp.StatusCode, "body", string(bodyBytes), "url", c.embeddingsServerURL, "queries", nodes)
		
		grpcCode := util.HTTPStatusToGRPCCode(httpResp.StatusCode)

		errMsg := string(bodyBytes)
		if errMsg == "" {
			errMsg = "The resolution service encountered an error processing your request."
		}
		return nil, status.Errorf(grpcCode, "Embeddings server error: %s", errMsg)
	}

	// Parse the response
	var searchResp searchVarsResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&searchResp); err != nil {
		slog.Error("Failed to decode embeddings server response", "error", err, "url", c.embeddingsServerURL)
		return nil, status.Errorf(codes.Internal, "An internal error occurred while parsing the resolution response.")
	}
	return &searchResp, nil
}

// buildResolveResponse converts the raw search response into the gRPC ResolveResponse.
func buildResolveResponse(
	ctx context.Context,
	nodes []string,
	searchResp *searchVarsResponse,
	typeOfValues []string,
	topicExpander TopicExpander,
	expandTopics bool,
) *pbv2.ResolveResponse {
	resolveResponse := &pbv2.ResolveResponse{
		Entities: make([]*pbv2.ResolveResponse_Entity, 0, len(nodes)),
	}

	for _, node := range nodes {
		entity := &pbv2.ResolveResponse_Entity{
			Node: node,
		}

		if result, ok := searchResp.QueryResults[node]; ok {
			entity.Candidates = buildEntityCandidates(ctx, &result, typeOfValues, topicExpander, expandTopics)
		}
		resolveResponse.Entities = append(resolveResponse.Entities, entity)
	}
	return resolveResponse
}

// buildEntityCandidates constructs a list of ResolveResponse_Entity_Candidate from a single searchResult.
func buildEntityCandidates(
	ctx context.Context,
	result *searchResult,
	typeOfValues []string,
	topicExpander TopicExpander,
	expandTopics bool,
) []*pbv2.ResolveResponse_Entity_Candidate {
	svInfos := fetchSVPropertyInfos(ctx, topicExpander, result, expandTopics)

	candidates := make([]*pbv2.ResolveResponse_Entity_Candidate, 0, len(result.SV))
	for i, statVarDcid := range result.SV {
		if i >= len(result.CosineScore) {
			break
		}

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
			candidate.Metadata["sentence"] = sentences[0].Sentence
		}

		if dominantType == TopicDominantType {
			if topicExpander != nil {
				candidate.Name = topicExpander.GetTopicDisplayName(ctx, statVarDcid)
				children, err := topicExpander.ExpandTopic(ctx, statVarDcid, expandTopics)
				if err != nil {
					slog.Error("Failed to expand topic during embedding resolution", "topic", statVarDcid, "error", err)
				}
				candidate.Children = children
			}
		} else {
			if svInfos != nil {
				if info, ok := svInfos[statVarDcid]; ok {
					candidate.Name = info.Name
					candidate.ObservationProperties = info.ObservationProperties
				}
			}
		}

		candidates = append(candidates, candidate)
	}
	return candidates
}

// fetchSVPropertyInfos aggregates non-topic DCIDs and expands topic DCIDs to pre-fetch their property info in batch.
func fetchSVPropertyInfos(ctx context.Context, topicExpander TopicExpander, result *searchResult, expandTopics bool) map[string]SVPropertyInfo {
	if topicExpander == nil || result == nil {
		return nil
	}
	var svDcidsToFetch []string
	for i, statVarDcid := range result.SV {
		// Stop collecting if trailing SVs lack matching CosineScores, as they will be ignored in candidate assembly.
		if i >= len(result.CosineScore) {
			break
		}
		if strings.Contains(statVarDcid, TopicDcidSubstring) {
			childSVs := topicExpander.GetTopicTargetSVs(ctx, statVarDcid, expandTopics)
			svDcidsToFetch = append(svDcidsToFetch, childSVs...)
		} else {
			svDcidsToFetch = append(svDcidsToFetch, statVarDcid)
		}
	}
	if len(svDcidsToFetch) == 0 {
		return nil
	}
	svInfos, err := topicExpander.GetSVPropertyInfos(ctx, svDcidsToFetch)
	if err != nil {
		slog.Error("Failed to fetch SV property infos during embedding resolution", "error", err, "dcids", svDcidsToFetch)
		return nil
	}
	return svInfos
}


