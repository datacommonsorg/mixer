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

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
)

// SQLDataSource represents a data source that interacts with SQL.
type SQLDataSource struct {
	client *SQLClient
}

func NewSQLDataSource(client *SQLClient) *SQLDataSource {
	return &SQLDataSource{client: client}
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
func (sds *SQLDataSource) Node(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	return nil, fmt.Errorf("unimplemented")
}

// Observation retrieves observation data from SQL.
func (sds *SQLDataSource) Observation(ctx context.Context, req *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
	return nil, fmt.Errorf("unimplemented")
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
