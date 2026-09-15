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
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// fetchAllChunkSize is how many nodes go into each chunk. Paging through one long
// list of nodes is slow, so we split the list up and fetch the chunks in parallel.
// Each chunk still pages on its own, so this only affects speed, not correctness.
const fetchAllChunkSize = 200

// NodeAllFetcher defines the contract for fetching all pages of V2 Node responses.
type NodeAllFetcher interface {
	NodeFetchAll(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error)
}

// FuncNodeFetcher adapts a V2 Node query function to the NodeAllFetcher interface.
type FuncNodeFetcher struct {
	fetchFunc func(context.Context, *pbv2.NodeRequest) (*pbv2.NodeResponse, error)
}

// NewFuncNodeFetcher creates a new FuncNodeFetcher wrapping the provided function.
func NewFuncNodeFetcher(fetchFunc func(context.Context, *pbv2.NodeRequest) (*pbv2.NodeResponse, error)) *FuncNodeFetcher {
	return &FuncNodeFetcher{fetchFunc: fetchFunc}
}

// NodeFetchAll implements NodeAllFetcher.
func (f *FuncNodeFetcher) NodeFetchAll(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	return NodeFetchAllFunc(ctx, f.fetchFunc, req)
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

// NodeFetchAllChunkedFunc fetches all pages of a NodeRequest. Node lists longer
// than fetchAllChunkSize are split into chunks and fetched in parallel, shorter
// ones are just paged sequentially.
func NodeFetchAllChunkedFunc(ctx context.Context, fetch func(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error), req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	nodes := req.GetNodes()
	if len(nodes) <= fetchAllChunkSize {
		return NodeFetchAllFunc(ctx, fetch, req)
	}

	chunks := chunkNodes(nodes, fetchAllChunkSize)
	slog.Debug("Partitioning large multi-node request into parallel chunks",
		"totalNodes", len(nodes),
		"chunkSize", fetchAllChunkSize,
		"totalChunks", len(chunks),
	)
	responses, err := fetchChunksParallel(ctx, fetch, req, chunks)
	if err != nil {
		return nil, err
	}

	return mergeDisjointResponses(responses), nil
}

// fetchChunksParallel fetches node metadata for partitioned chunks in parallel.
func fetchChunksParallel(
	ctx context.Context,
	fetch func(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error),
	req *pbv2.NodeRequest,
	chunks [][]string,
) ([]*pbv2.NodeResponse, error) {
	g, groupCtx := errgroup.WithContext(ctx)
	responses := make([]*pbv2.NodeResponse, len(chunks))

	for idx, chunk := range chunks {
		chunkIdx, chunkNodes := idx, chunk
		g.Go(func() error {
			chunkReq := proto.Clone(req).(*pbv2.NodeRequest)
			chunkReq.Nodes = chunkNodes

			resp, err := NodeFetchAllFunc(groupCtx, fetch, chunkReq)
			if err != nil {
				return err
			}
			responses[chunkIdx] = resp
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return responses, nil
}

// chunkNodes partitions a string slice into disjoint chunks of the specified size.
func chunkNodes(nodes []string, size int) [][]string {
	var chunks [][]string
	for i := 0; i < len(nodes); i += size {
		end := i + size
		if end > len(nodes) {
			end = len(nodes)
		}
		chunks = append(chunks, nodes[i:end])
	}
	return chunks
}

// mergeDisjointResponses deep-merges disjoint parallel NodeResponses into a single response.
func mergeDisjointResponses(responses []*pbv2.NodeResponse) *pbv2.NodeResponse {
	accumulated := &pbv2.NodeResponse{
		Data: make(map[string]*pbv2.LinkedGraph),
	}
	for _, resp := range responses {
		if resp == nil {
			continue
		}
		for k, v := range resp.GetData() {
			accumulated.Data[k] = v
		}
	}
	return accumulated
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
