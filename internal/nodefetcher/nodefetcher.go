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

// Package nodefetcher isolates the high-level contract and pagination aggregation logic
// for fetching complete Node responses across all underlying backends.
// This package was created to decouple consumers from concrete backend dependencies.
package nodefetcher

import (
	"context"
	"fmt"
	"log/slog"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"google.golang.org/protobuf/proto"
)

// NodeAllFetcher defines the contract for fetching all pages of V2 Node responses.
type NodeAllFetcher interface {
	NodeFetchAll(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error)
}

// NodeFetchAllFunc fetches all NodeResponse pages for a given request by repeatedly calling a fetch closure
// as long as a NextToken is returned and merges into single response.
func NodeFetchAllFunc(ctx context.Context, fetch func(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error), req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	// Make initial call.
	resp, err := fetch(ctx, req)
	if err != nil {
		slog.Error("NodeFetchAllFunc: initial fetch failed", "error", err)
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("NodeFetchAllFunc: initial fetch returned nil response")
	}

	if resp.NextToken == "" {
		return resp, nil
	}

	// Clone the request to avoid modifying the caller's object and prevent data races.
	reqClone := proto.Clone(req).(*pbv2.NodeRequest)

	// Initialize accumulated response with data from the first page.
	accumulatedResp := resp

	for accumulatedResp.NextToken != "" {
		reqClone.NextToken = accumulatedResp.NextToken

		nextResp, err := fetch(ctx, reqClone)
		if err != nil {
			slog.Error("NodeFetchAllFunc: subsequent fetch failed", "nextToken", reqClone.NextToken, "error", err)
			return nil, err
		}
		if nextResp == nil {
			return nil, fmt.Errorf("NodeFetchAllFunc: subsequent fetch returned nil response")
		}

		// Capture next token before merging.
		nextToken := nextResp.NextToken

		// Manual deep merge to avoid proto.Merge issues (which failed in tests by overwriting).
		if accumulatedResp.Data == nil {
			accumulatedResp.Data = make(map[string]*pbv2.LinkedGraph)
		}

		for subjectID, newGraph := range nextResp.Data {
			accumulatedGraph, ok := accumulatedResp.Data[subjectID]
			if !ok {
				accumulatedResp.Data[subjectID] = newGraph
				continue
			}

			if accumulatedGraph.Arcs == nil {
				accumulatedGraph.Arcs = make(map[string]*pbv2.Nodes)
			}

			for prop, newNodes := range newGraph.Arcs {
				accumulatedNodes, ok := accumulatedGraph.Arcs[prop]
				if !ok {
					accumulatedGraph.Arcs[prop] = newNodes
					continue
				}
				accumulatedNodes.Nodes = append(accumulatedNodes.Nodes, newNodes.Nodes...)
			}
		}

		accumulatedResp.NextToken = nextToken
	}

	return accumulatedResp, nil
}
