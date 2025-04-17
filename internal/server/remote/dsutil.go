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

// Utility functions used by the RemoteDataSource.

package remote

import (
	"fmt"

	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/pagination"
	"github.com/datacommonsorg/mixer/internal/util"
)

// formatNodeRequest updates the NodeRequest for a remote data source.
func formatNodeRequest(req *pbv2.NodeRequest, id string) error {
	if req.GetNextToken() != "" {
		info, err := pagination.DecodeNextToken(req.GetNextToken())
		if err != nil {
			return err
		}

		req.NextToken = ""
		for _, dataSourceInfo := range info.Info {
			if dataSourceInfo.GetId() == id {
				remote_info, ok := dataSourceInfo.GetDataSourceInfo().(*pbv2.Pagination_DataSourceInfo_BigtableInfo)
				if !ok {
					return fmt.Errorf("found different data source info for remote data source id: %s", id)
				}

				pi := &pbv1.PaginationInfo{
					RemotePaginationInfo: remote_info.BigtableInfo,
				}
				nextToken, err := util.EncodeProto(pi)
				if err != nil {
					return err
				}

				req.NextToken = nextToken
			}
		}
	}
	return nil
}

// formatNodeResponse updates the NodeResponse from a remote data source.
func formatNodeResponse(resp *pbv2.NodeResponse, id string) error {
	if resp.GetNextToken() == "" {
		return nil
	}

	pi, err := pagination.Decode(resp.GetNextToken())
	if err != nil {
		return err
	}

	info := &pbv2.Pagination{
		Info: []*pbv2.Pagination_DataSourceInfo{
			{
				Id: id,
				DataSourceInfo: &pbv2.Pagination_DataSourceInfo_BigtableInfo{
					BigtableInfo: pi,
				},
			},
		},
	}
	nextToken, err := util.EncodeProto(info)
	if err != nil {
		return err
	}

	resp.NextToken = nextToken
	return nil
}
