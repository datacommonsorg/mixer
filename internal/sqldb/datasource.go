// Copyright 2025 Google LLC
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

package sqldb

import (
	"context"
	"fmt"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
)

// SQLDataSource represents a data source that interacts with SQL.
type SQLDataSource struct {
	client *SQLClient
	// The secondary data source is used to fetch certain data (e.g. child places) that is not available in SQL.
	// If one is not configured, those calls will be skipped.
	// The secondary data source is typically a remote data source but it could be a different data source (like spanner) in tests.
	secondaryDataSource datasource.DataSource
}

func NewSQLDataSource(client *SQLClient, secondaryDataSource datasource.DataSource) *SQLDataSource {
	return &SQLDataSource{client: client, secondaryDataSource: secondaryDataSource}
}

// Type returns the type of the data source.
func (sds *SQLDataSource) Type() datasource.DataSourceType {
	return datasource.TypeSQL
}

// Id returns the id of the data source.
func (sds *SQLDataSource) Id() string {
	return fmt.Sprintf("%s-%s", string(sds.Type()), sds.client.id)
}

// Node retrieves node data from SQL.
func (sds *SQLDataSource) Node(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
	// SQLClient currently doesn't have pagination, so return if there's a nextToken,
	// since all results would have been returned in the first page.
	if req.GetNextToken() != "" {
		return &pbv2.NodeResponse{}, nil
	}

	arcs, err := v2.ParseProperty(req.GetProperty())
	if err != nil {
		return nil, err
	}
	if len(arcs) == 0 {
		return &pbv2.NodeResponse{}, nil
	}
	if len(arcs) > 1 {
		return nil, fmt.Errorf("multiple arcs in node request")
	}
	arc := arcs[0]

	if arc.IsNodePropertiesArc() {
		nodePredicates, err := sds.client.GetNodePredicates(ctx, req.Nodes, arc.Direction())
		if err != nil {
			return nil, fmt.Errorf("error getting node predicates: %v", err)
		}
		return nodePredicatesToNodeResponse(nodePredicates), nil
	}

	if ok, properties := arc.IsPropertyValuesArc(); ok {
		if len(properties) == 0 {
			nodePredicates, err := sds.client.GetNodePredicates(ctx, req.Nodes, arc.Direction())
			if err != nil {
				return nil, err
			}
			properties = nodePredicatesToProperties(nodePredicates)
		}
		nodeTriples, err := sds.client.GetNodeTriples(ctx, req.Nodes, properties, arc.Direction())
		if err != nil {
			return nil, err
		}
		entityInfoTriples, err := sds.client.GetEntityInfoTriples(ctx, collectDcids(nodeTriples))
		if err != nil {
			return nil, err
		}
		return triplesToNodeResponse(nodeTriples, entityInfoTriples, arc.Direction()), nil
	}

	// TODO: Add support for other types of node requests.

	return &pbv2.NodeResponse{}, nil
}

// Observation retrieves observation data from SQL.
func (sds *SQLDataSource) Observation(ctx context.Context, req *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
	return &pbv2.ObservationResponse{}, nil
}

// NodeSearch searches nodes in the SQL database.
// However, node search is not currently supported for SQL data sources so it returns an empty response.
func (sds *SQLDataSource) NodeSearch(ctx context.Context, req *pbv2.NodeSearchRequest) (*pbv2.NodeSearchResponse, error) {
	return &pbv2.NodeSearchResponse{}, nil
}

// Resolve searches for nodes in the graph.
// However, resolve is not currently supported for SQL data sources so it returns an empty response.
func (sds *SQLDataSource) Resolve(ctx context.Context, req *pbv2.ResolveRequest) (*pbv2.ResolveResponse, error) {
	return &pbv2.ResolveResponse{}, nil
}

// Sparql executes a SPARQL query against the data source.
// However, Sparql is not currently supported for SQL data sources so it returns an empty response.
func (sds *SQLDataSource) Sparql(ctx context.Context, req *pb.SparqlRequest) (*pb.QueryResponse, error) {
	return &pb.QueryResponse{}, nil
}
