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

package remote

import (
	"context"

	pbv3 "github.com/datacommonsorg/mixer/internal/proto/v3"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
)

// RemoteDataSource represents a data source that interacts with a Remote Mixer.
type RemoteDataSource struct {
	client *RemoteClient
}

func NewRemoteDataSource(client *RemoteClient) *RemoteDataSource {
	return &RemoteDataSource{client: client}
}

// Type returns the type of the data source.
func (rds *RemoteDataSource) Type() datasource.DataSourceType {
	return datasource.TypeRemote
}

func (rds *RemoteDataSource) Node(ctx context.Context, req *pbv3.NodeRequest) (*pbv3.NodeResponse, error) {
	return rds.client.Node(req)
}

func (rds *RemoteDataSource) Observation(ctx context.Context, req *pbv3.ObservationRequest) (*pbv3.ObservationResponse, error) {
	return rds.client.Observation(req)
}

func (rds *RemoteDataSource) NodeSearch(ctx context.Context, req *pbv3.NodeSearchRequest) (*pbv3.NodeSearchResponse, error) {
	return rds.client.NodeSearch(req)
}

func (rds *RemoteDataSource) Resolve(ctx context.Context, req *pbv3.ResolveRequest) (*pbv3.ResolveResponse, error) {
	return rds.client.Resolve(req)
}
