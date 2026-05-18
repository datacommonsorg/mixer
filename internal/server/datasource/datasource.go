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

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/nodefetcher"
)

// DataSourceType represents the type of data source.
type DataSourceType string

const (
	TypeRemote  DataSourceType = "remote"
	TypeSpanner DataSourceType = "spanner"
	TypeSQL     DataSourceType = "sql"
	TypeMock    DataSourceType = "mock"
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
}

// NodeFetchAll fetches all NodeResponse pages for a given request by repeatedly calling ds.Node
// as long as a NextToken is returned and merges into single response.
func NodeFetchAll(ctx context.Context, ds DataSource, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
	if pageSize <= 0 {
		return nil, fmt.Errorf("pageSize must be positive")
	}
	return nodefetcher.NodeFetchAllFunc(ctx, func(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
		return ds.Node(ctx, req, pageSize)
	}, req)
}


