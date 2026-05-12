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
	"fmt"

	"github.com/datacommonsorg/mixer/internal/merger"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/server/v2/resolve"
	"github.com/datacommonsorg/mixer/internal/translator/sparql"
	"google.golang.org/protobuf/proto"

	"golang.org/x/sync/errgroup"
)

const (
	// Default page size for paginated responses.
	DefaultPageSize = 500
)

// DataSources struct uses underlying data sources to respond to API requests.
type DataSources struct {
	sources          []datasource.DataSource
	remoteDataSource datasource.DataSource
}

func NewDataSources(sources []datasource.DataSource, remoteDataSource datasource.DataSource) *DataSources {
	return &DataSources{sources, remoteDataSource}
}

// GetSources returns the list of data source IDs.
func (ds *DataSources) GetSources() []string {
	sources := make([]string, 0, len(ds.sources))
	for _, source := range ds.sources {
		sources = append(sources, source.Id())
	}
	return sources
}

// fetchAndMerge is a generic helper that fetches data from multiple sources in parallel
// and merges the responses using the provided fetcher and merger functions.
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
	expandedReq := in
	hasLocalSource := false
	for _, s := range ds.sources {
		if s.Type() != datasource.TypeRemote {
			hasLocalSource = true
			break
		}
	}
	if in.Entity != nil && in.Entity.Expression != "" && ds.remoteDataSource != nil && hasLocalSource {
		// When a remote mixer is present, we need to ensure that local sources fetch data
		// for entities discovered by the remote mixer as well.
		// We do this by resolving the expression against all sources (local + remote) first,
		// and then passing the resolved DCIDs to the local sources.

		// 1. Parse expression to extract ancestor and child type.
		containedInPlace, err := v2.ParseContainedInPlace(in.Entity.Expression)
		if err != nil {
			return nil, err
		}

		// 2. Resolve expression via Node API.
		// This calls all configured sources (local and remote) in parallel and merges results.
		// This gives us a unified list of entities across both graphs before executing the local observation query.
		property := fmt.Sprintf("<-containedInPlace+{typeOf:%s}", containedInPlace.ChildPlaceType)
		nodeReq := &pbv2.NodeRequest{
			Nodes:    []string{containedInPlace.Ancestor},
			Property: property,
		}
		nodeResp, err := ds.Node(ctx, nodeReq, 0)
		if err != nil {
			return nil, err
		}

		// 3. Extract DCIDs from NodeResponse.
		var dcids []string
		for _, graph := range nodeResp.Data {
			if nodes, ok := graph.Arcs[property]; ok {
				for _, node := range nodes.Nodes {
					if node.Dcid != "" {
						dcids = append(dcids, node.Dcid)
					}
				}
			}
		}

		// 4. Short-circuit if empty to avoid backend errors.
		if len(dcids) == 0 {
			return &pbv2.ObservationResponse{}, nil
		}

		// 5. Create cloned request for local sources with resolved DCIDs.
		// The remote mixer must receive the original expression to do its own expansion.
		// Local sources get the resolved list of DCIDs and have the expression cleared
		// to prevent them from doing their own un-federated expansion or failing if both are set.
		expandedReq = proto.Clone(in).(*pbv2.ObservationRequest)
		expandedReq.Entity.Dcids = dcids
		expandedReq.Entity.Expression = ""
	}

	return fetchAndMerge(ctx, ds.sources, in,
		func(c context.Context, s datasource.DataSource, r *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
			reqForSource := r
			// Send the expanded DCIDs request to all local (non-remote) sources.
			// Send the original request (with expression) to the remote mixer.
			if s.Type() != datasource.TypeRemote {
				reqForSource = expandedReq
			}
			return s.Observation(c, reqForSource)
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
	filteredResolveSources := filterResolveSources(ds, in)

	return fetchAndMerge(ctx, filteredResolveSources, in,
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

func (ds *DataSources) Event(ctx context.Context, in *pbv2.EventRequest) (*pbv2.EventResponse, error) {
	return fetchAndMerge(ctx, ds.sources, in,
		func(c context.Context, s datasource.DataSource, r *pbv2.EventRequest) (*pbv2.EventResponse, error) {
			return s.Event(c, r)
		},
		func(all []*pbv2.EventResponse) (*pbv2.EventResponse, error) {
			return merger.MergeMultiEvent(all), nil
		},
	)
}

func (ds *DataSources) BulkVariableInfo(ctx context.Context, in *pbv1.BulkVariableInfoRequest) (*pbv1.BulkVariableInfoResponse, error) {
	return fetchAndMerge(ctx, ds.sources, in,
		func(c context.Context, s datasource.DataSource, r *pbv1.BulkVariableInfoRequest) (*pbv1.BulkVariableInfoResponse, error) {
			return s.BulkVariableInfo(c, r)
		},
		func(all []*pbv1.BulkVariableInfoResponse) (*pbv1.BulkVariableInfoResponse, error) {
			return merger.MergeMultiBulkVariableInfo(all), nil
		},
	)
}

func (ds *DataSources) BulkVariableGroupInfo(ctx context.Context, in *pbv1.BulkVariableGroupInfoRequest) (*pbv1.BulkVariableGroupInfoResponse, error) {
	return fetchAndMerge(ctx, ds.sources, in,
		func(c context.Context, s datasource.DataSource, r *pbv1.BulkVariableGroupInfoRequest) (*pbv1.BulkVariableGroupInfoResponse, error) {
			return s.BulkVariableGroupInfo(c, r)
		},
		func(all []*pbv1.BulkVariableGroupInfoResponse) (*pbv1.BulkVariableGroupInfoResponse, error) {
			return merger.MergeMultiBulkVariableGroupInfo(all), nil
		},
	)
}

// Resolve API specifies which sources to call in the target input params.
// filterResolveSources filters sources accordingly.
func filterResolveSources(ds *DataSources, in *pbv2.ResolveRequest) []datasource.DataSource {
	hasRemoteMixerDomain := ds.remoteDataSource != nil

	callLocal, callRemote := resolve.ResolveRouting(in.GetTarget(), hasRemoteMixerDomain)
	var filteredSources []datasource.DataSource
	for _, source := range ds.sources {
		isRemote := source == ds.remoteDataSource
		if (isRemote && callRemote) || (!isRemote && callLocal) {
			filteredSources = append(filteredSources, source)
		}
	}

	return filteredSources
}

func (ds *DataSources) SdmxData(ctx context.Context, in *pb.SdmxDataQuery) (*pb.SdmxDataResult, error) {
	return fetchAndMerge(ctx, ds.sources, in,
		func(c context.Context, s datasource.DataSource, r *pb.SdmxDataQuery) (*pb.SdmxDataResult, error) {
			return s.SdmxData(c, r)
		},
		func(all []*pb.SdmxDataResult) (*pb.SdmxDataResult, error) {
			res := &pb.SdmxDataResult{}
			for _, result := range all {
				if result != nil && result.Observations != nil {
					res.Observations = append(res.Observations, result.Observations...)
				}
			}
			return res, nil
		},
	)
}
