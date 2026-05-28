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

package datasource

import (
	"context"
	"fmt"
	"log/slog"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/nodefetcher"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// DataSourceType represents the type of data source.
type DataSourceType string

const (
	TypeRemote  DataSourceType = "remote"
	TypeSpanner DataSourceType = "spanner"
	TypeSQL     DataSourceType = "sql"
	TypeMock    DataSourceType = "mock"
)

const (
	// defaultFetchAllPageSize is the default page size for NodeFetchAll sequential queries.
	defaultFetchAllPageSize = 1000

	// fetchAllChunkSize is the size of node partitions used to chunk multi-node NodeFetchAll requests.
	// Spanner's NodeFetcher limits returned edges to a strict threshold of 1,000 rows per query.
	// While NodeFetchAll pages sequentially using NextToken, doing so for a massive list of
	// independent nodes is highly inefficient. More importantly, if a query matches thousands of
	// edges across multiple subject nodes, Spanner truncates the returned results within a page,
	// silently dropping properties for alphabetically later nodes. Chunking the requested nodes
	// into independent parallel batches guarantees we remain well below Spanner's truncation threshold
	// (e.g., 200 nodes * 3 properties = 600 edges) while maximizing retrieval performance.
	fetchAllChunkSize = 200
)

// DataSource interface defines the common methods for all data sources.
type DataSource interface {
	Type() DataSourceType
	Id() string
	Node(context.Context, *pbv2.NodeRequest, int) (*pbv2.NodeResponse, error)
	Observation(context.Context, *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error)
	NodeSearch(context.Context, *pbv2.NodeSearchRequest) (*pbv2.NodeSearchResponse, error)
	Resolve(context.Context, *pbv2.ResolveRequest) (*pbv2.ResolveResponse, error)
	Sparql(context.Context, *pb.SparqlRequest) (*pb.QueryResponse, error)
	Event(context.Context, *pbv2.EventRequest) (*pbv2.EventResponse, error)
	BulkVariableInfo(context.Context, *pbv1.BulkVariableInfoRequest) (*pbv1.BulkVariableInfoResponse, error)
	BulkVariableGroupInfo(context.Context, *pbv1.BulkVariableGroupInfoRequest) (*pbv1.BulkVariableGroupInfoResponse, error)
	SdmxData(context.Context, *pb.SdmxDataQuery) (*pb.SdmxDataResult, error)
	FilterStatVarsByEntity(context.Context, *pb.FilterStatVarsByEntityRequest) (*pb.FilterStatVarsByEntityResponse, error)
}

// NodeFetchAll fetches all NodeResponse pages for a given request by repeatedly calling ds.Node
// as long as a NextToken is returned and merges into single response.
// It automatically partitions multi-node requests into chunks of fetchAllChunkSize and executes
// them in parallel to prevent exceeding database pagination/truncation limits and maximize performance.
func NodeFetchAll(ctx context.Context, ds DataSource, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
	if pageSize <= 0 {
		return nil, fmt.Errorf("pageSize must be positive")
	}

	nodes := req.GetNodes()

	// If it's a single-node request, bypass chunking and page sequentially
	if len(nodes) <= fetchAllChunkSize {
		return nodefetcher.NodeFetchAllFunc(ctx, func(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
			return ds.Node(ctx, req, pageSize)
		}, req)
	}

	chunks := chunkNodes(nodes, fetchAllChunkSize)
	slog.Debug("Partitioning large multi-node request into parallel chunks",
		"totalNodes", len(nodes),
		"chunkSize", fetchAllChunkSize,
		"totalChunks", len(chunks),
	)
	responses, err := fetchChunksParallel(ctx, ds, req, chunks, pageSize)
	if err != nil {
		return nil, err
	}

	return mergeDisjointResponses(responses), nil
}

// fetchChunksParallel fetches node metadata for partitioned chunks in parallel.
func fetchChunksParallel(
	ctx context.Context,
	ds DataSource,
	req *pbv2.NodeRequest,
	chunks [][]string,
	pageSize int,
) ([]*pbv2.NodeResponse, error) {
	g, groupCtx := errgroup.WithContext(ctx)
	responses := make([]*pbv2.NodeResponse, len(chunks))

	for idx, chunk := range chunks {
		chunkIdx, chunkNodes := idx, chunk
		g.Go(func() error {
			chunkReq := proto.Clone(req).(*pbv2.NodeRequest)
			chunkReq.Nodes = chunkNodes

			resp, err := nodefetcher.NodeFetchAllFunc(groupCtx, func(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
				return ds.Node(ctx, req, pageSize)
			}, chunkReq)
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

// NewNodeFetcher returns a nodefetcher.NodeAllFetcher for the given DataSource using the default page size.
func NewNodeFetcher(ds DataSource) nodefetcher.NodeAllFetcher {
	return nodefetcher.NewFuncNodeFetcher(func(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
		return NodeFetchAll(ctx, ds, req, defaultFetchAllPageSize)
	})
}


