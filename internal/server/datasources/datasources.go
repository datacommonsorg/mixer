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

package datasources

import (
	"context"

	"github.com/datacommonsorg/mixer/internal/merger"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/translator/sparql"

	"golang.org/x/sync/errgroup"
)

const (
	// Default page size for paginated responses.
	DefaultPageSize = 500
)

// DataSources struct uses underlying data sources to respond to API requests.
type DataSources struct {
	sources []datasource.DataSource
}

func NewDataSources(sources []datasource.DataSource) *DataSources {
	return &DataSources{sources: sources}
}

// GetSources returns the list of data source IDs.
func (ds *DataSources) GetSources() []string {
	sources := make([]string, 0, len(ds.sources))
	for _, source := range ds.sources {
		sources = append(sources, source.Id())
	}
	return sources
}

func fetchAndMerge[req any, resp any](
	ctx context.Context,
	sources []datasource.DataSource,
	in req,
	fetcher func(context.Context, datasource.DataSource, req) (resp, error),
	merger func([]resp) (resp, error),
) (resp, error) {
	errGroup, errCtx := errgroup.WithContext(ctx)
	resps := make([]resp, len(sources))

	for i, source := range sources {
		i, src := i, source
		errGroup.Go(func() error {
			resp, err := fetcher(errCtx, src, in)
			if err != nil {
				return err
			}
			resps[i] = resp
			return nil
		})
	}

	var zero resp
	if err := errGroup.Wait(); err != nil {
		return zero, err
	}

	return merger(resps)
}

func (ds *DataSources) Node(ctx context.Context, in *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
	return fetchAndMerge(ctx, ds.sources, in,
		func(c context.Context, s datasource.DataSource, r *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
			return s.Node(c, r, pageSize)
		},
		merger.MergeMultiNode,
	)
}

func (ds *DataSources) Observation(ctx context.Context, in *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
	return fetchAndMerge(ctx, ds.sources, in,
		func(c context.Context, s datasource.DataSource, r *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
			return s.Observation(c, r)
		},
		func(all []*pbv2.ObservationResponse) (*pbv2.ObservationResponse, error) {
			return merger.MergeMultiObservation(all), nil
		},
	)
}

func (ds *DataSources) NodeSearch(ctx context.Context, in *pbv2.NodeSearchRequest) (*pbv2.NodeSearchResponse, error) {
	return fetchAndMerge(ctx, ds.sources, in,
		func(c context.Context, s datasource.DataSource, r *pbv2.NodeSearchRequest) (*pbv2.NodeSearchResponse, error) {
			return s.NodeSearch(c, r)
		},
		merger.MergeMultiNodeSearch,
	)
}

func (ds *DataSources) Resolve(ctx context.Context, in *pbv2.ResolveRequest) (*pbv2.ResolveResponse, error) {
	return fetchAndMerge(ctx, ds.sources, in,
		func(c context.Context, s datasource.DataSource, r *pbv2.ResolveRequest) (*pbv2.ResolveResponse, error) {
			return s.Resolve(c, r)
		},
		func(all []*pbv2.ResolveResponse) (*pbv2.ResolveResponse, error) {
			return merger.MergeMultiResolve(all), nil
		},
	)
}

func (ds *DataSources) Sparql(ctx context.Context, in *pb.SparqlRequest) (*pb.QueryResponse, error) {
	_, _, opts, err := sparql.ParseQuery(in.GetQuery())
	if err != nil {
		return nil, err
	}

	return fetchAndMerge(ctx, ds.sources, in,
		func(c context.Context, s datasource.DataSource, r *pb.SparqlRequest) (*pb.QueryResponse, error) {
			return s.Sparql(c, r)
		},
		func(all []*pb.QueryResponse) (*pb.QueryResponse, error) {
			return merger.MergeMultiQueryResponse(all, opts.Orderby, opts.ASC, opts.Limit)
		},
	)
}
