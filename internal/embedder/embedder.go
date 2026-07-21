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

package embedder

import (
	"context"
	"fmt"

	"google.golang.org/genai"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const defaultGenAILocation = "us-central1"

// Embedder generates vector embeddings for input text.
type Embedder interface {
	Embed(ctx context.Context, endpoint, taskType, text string) ([]float64, error)
}

type genAIEmbedder struct {
	client *genai.Client
}

func (e *genAIEmbedder) Embed(ctx context.Context, endpoint, taskType, text string) ([]float64, error) {
	if e.client == nil {
		return nil, status.Errorf(codes.Internal, "GenAI client is not initialized")
	}
	if endpoint == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Embedding model endpoint is required")
	}
	res, err := e.client.Models.EmbedContent(
		ctx,
		endpoint,
		genai.Text(text),
		&genai.EmbedContentConfig{TaskType: taskType},
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get term embedding for %s via GenAI SDK: %v", text, err)
	}
	if len(res.Embeddings) > 0 && res.Embeddings[0] != nil {
		embeddings := make([]float64, len(res.Embeddings[0].Values))
		for idx, val := range res.Embeddings[0].Values {
			embeddings[idx] = float64(val)
		}
		return embeddings, nil
	}
	return nil, nil
}

// NewEmbedder creates a new Embedder instance wrapping Google GenAI SDK client.
func NewEmbedder(ctx context.Context, projectID, location string) (Embedder, error) {
	if projectID == "" {
		return nil, fmt.Errorf("projectID is required")
	}
	if location == "" {
		location = defaultGenAILocation
	}
	client, err := genai.NewClient(ctx, &genai.ClientConfig{
		Project:  projectID,
		Location: location,
		Backend:  genai.BackendVertexAI,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize GenAI client: %w", err)
	}
	return &genAIEmbedder{client: client}, nil
}
