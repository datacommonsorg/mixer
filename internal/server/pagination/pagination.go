// Copyright 2022 Google LLC
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

package pagination

import (
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	// maxRemotePaginationDepth bounds recursive nesting of RemotePaginationInfo.
	maxRemotePaginationDepth = 5
)

// Decode decodes a compressed token string into PaginationInfo.
func Decode(s string) (*pbv1.PaginationInfo, error) {
	if s == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty pagination token string")
	}

	data, err := util.UnzipAndDecode(s)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid pagination token: %v", err)
	}
	result := &pbv1.PaginationInfo{}
	err = proto.Unmarshal(data, result)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "malformed pagination token: %v", err)
	}
	if err := validatePaginationInfo(result, 0); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid pagination token: %v", err)
	}
	return result, nil
}

// Decode decodes a compressed token string into Pagination.
func DecodeNextToken(s string) (*pbv2.Pagination, error) {
	if s == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty pagination token string")
	}

	data, err := util.UnzipAndDecode(s)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid pagination token: %v", err)
	}
	result := &pbv2.Pagination{}
	err = proto.Unmarshal(data, result)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "malformed pagination token: %v", err)
	}
	if err := validatePagination(result); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid pagination token: %v", err)
	}
	return result, nil
}

func validatePaginationInfo(pi *pbv1.PaginationInfo, depth int) error {
	if pi == nil {
		return status.Errorf(codes.InvalidArgument, "pagination info must not be nil")
	}
	if depth > maxRemotePaginationDepth {
		return status.Errorf(codes.InvalidArgument, "remote pagination info exceeds maximum depth of %d", maxRemotePaginationDepth)
	}
	for _, cg := range pi.GetCursorGroups() {
		if cg == nil {
			return status.Errorf(codes.InvalidArgument, "cursor group must not be nil")
		}
		for _, c := range cg.GetCursors() {
			if c == nil {
				return status.Errorf(codes.InvalidArgument, "cursor must not be nil")
			}
			if c.GetImportGroup() < 0 || c.GetPage() < 0 || c.GetItem() < 0 || c.GetOffset() < 0 {
				return status.Errorf(codes.InvalidArgument, "cursor values must be non-negative")
			}
		}
	}
	if pi.GetRemotePaginationInfo() != nil {
		return validatePaginationInfo(pi.GetRemotePaginationInfo(), depth+1)
	}
	return nil
}

func validatePagination(p *pbv2.Pagination) error {
	if p == nil {
		return status.Errorf(codes.InvalidArgument, "pagination must not be nil")
	}
	for _, dsi := range p.GetInfo() {
		if dsi == nil {
			return status.Errorf(codes.InvalidArgument, "data source info must not be nil")
		}
		switch info := dsi.GetDataSourceInfo().(type) {
		case *pbv2.Pagination_DataSourceInfo_SpannerInfo:
			if info.SpannerInfo == nil || info.SpannerInfo.GetOffset() < 0 {
				return status.Errorf(codes.InvalidArgument, "spanner offset must be non-negative")
			}
		case *pbv2.Pagination_DataSourceInfo_BigtableInfo:
			if info.BigtableInfo == nil {
				return status.Errorf(codes.InvalidArgument, "bigtable pagination info must not be nil")
			}
			if err := validatePaginationInfo(info.BigtableInfo, 0); err != nil {
				return err
			}
		case *pbv2.Pagination_DataSourceInfo_StringInfo:
			// Validated by the target data source when decoded.
		default:
			return status.Errorf(codes.InvalidArgument, "data source info must specify a valid source type")
		}
	}
	return nil
}
