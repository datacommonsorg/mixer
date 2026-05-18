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
	// Clone the request to avoid modifying the caller's object and prevent data races.
	reqClone := proto.Clone(req).(*pbv2.NodeRequest)

	var accumulatedResp *pbv2.NodeResponse

	for {
		resp, err := fetch(ctx, reqClone)
		if err != nil {
			if reqClone.NextToken == "" {
				return nil, fmt.Errorf("nodefetcher: fetch failed: %w", err)
			}
			return nil, fmt.Errorf("nodefetcher: fetch failed for token %s: %w", reqClone.NextToken, err)
		}
		if resp == nil {
			if reqClone.NextToken == "" {
				return nil, fmt.Errorf("nodefetcher: fetch returned nil response")
			}
			return nil, fmt.Errorf("nodefetcher: fetch returned nil response for token %s", reqClone.NextToken)
		}

		if accumulatedResp == nil {
			accumulatedResp = resp
		} else {
			mergeNodeResponse(accumulatedResp, resp)
		}

		if resp.NextToken == "" {
			break
		}
		reqClone.NextToken = resp.NextToken
	}

	return accumulatedResp, nil
}

// mergeNodeResponse deep merges the data maps from a new response page into the accumulated response.
func mergeNodeResponse(accumulated, new *pbv2.NodeResponse) {
	accumulated.NextToken = new.NextToken

	if accumulated.Data == nil {
		accumulated.Data = make(map[string]*pbv2.LinkedGraph)
	}

	for subjectID, newGraph := range new.Data {
		if newGraph == nil {
			continue
		}
		accumulatedGraph, ok := accumulated.Data[subjectID]
		if !ok || accumulatedGraph == nil {
			accumulated.Data[subjectID] = newGraph
			continue
		}

		if accumulatedGraph.Arcs == nil {
			accumulatedGraph.Arcs = make(map[string]*pbv2.Nodes)
		}

		for prop, newNodes := range newGraph.Arcs {
			if newNodes == nil {
				continue
			}
			accumulatedNodes, ok := accumulatedGraph.Arcs[prop]
			if !ok || accumulatedNodes == nil {
				accumulatedGraph.Arcs[prop] = newNodes
				continue
			}
			accumulatedNodes.Nodes = append(accumulatedNodes.Nodes, newNodes.Nodes...)
		}
	}
}
