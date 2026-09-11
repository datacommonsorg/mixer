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

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/pagination"
	"github.com/datacommonsorg/mixer/internal/util"
)

// updateNodeRequestNextToken rewrites req.NextToken to the cursor this remote
// source issued on the previous page, and reports whether that source has
// already returned all of its rows.
//
// MergeMultiNode drops a source from the merged token once that source stops
// returning a cursor, so a non-empty caller token with no entry for id means
// the source finished on an earlier page. Callers must not query a source
// reported as exhausted: doing so replays its first page.
//
// An empty caller token is the first page, so req.NextToken is left empty and
// isExhausted is false. On the exhausted path req is left untouched, so it
// still carries the caller's merged token: callers must not issue it.
func updateNodeRequestNextToken(req *pbv2.NodeRequest, id string) (isExhausted bool, err error) {
	if req.GetNextToken() == "" {
		return false, nil
	}

	info, err := pagination.DecodeNextToken(req.GetNextToken())
	if err != nil {
		return false, err
	}

	for _, dataSourceInfo := range info.Info {
		if dataSourceInfo.GetId() == id {
			stringInfo, ok := dataSourceInfo.GetDataSourceInfo().(*pbv2.Pagination_DataSourceInfo_StringInfo)
			if !ok {
				return false, fmt.Errorf("found different data source info for remote data source id: %s", id)
			}

			req.NextToken = stringInfo.StringInfo
			return false, nil
		}
	}

	return true, nil
}

// updateNodeResponseNextToken updates the NodeResponse nextToken from a remote data source.
func updateNodeResponseNextToken(resp *pbv2.NodeResponse, id string) error {
	if resp.GetNextToken() == "" {
		return nil
	}

	info := &pbv2.Pagination{
		Info: []*pbv2.Pagination_DataSourceInfo{
			{
				Id: id,
				DataSourceInfo: &pbv2.Pagination_DataSourceInfo_StringInfo{
					StringInfo: resp.GetNextToken(),
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
