// Copyright 2026 Google LLC
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

// Package server contains concrete node fetcher adapters (SpannerNodeFetcher and StoreNodeFetcher).
// This file was created to implement the nodefetcher.NodeAllFetcher interface for specific server storage backends
// without scattering adapter boilerplate across multiple packages.
package server

import (
	"context"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	fetcher "github.com/datacommonsorg/mixer/internal/nodefetcher"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
)

const spannerPageSize = 1000

// SpannerNodeFetcher wraps a Spanner DataSource to implement nodefetcher.NodeAllFetcher.
type SpannerNodeFetcher struct {
	ds datasource.DataSource
}

// NewSpannerNodeFetcher creates a new SpannerNodeFetcher.
func NewSpannerNodeFetcher(ds datasource.DataSource) *SpannerNodeFetcher {
	return &SpannerNodeFetcher{ds: ds}
}

// NodeFetchAll implements nodefetcher.NodeAllFetcher.
func (f *SpannerNodeFetcher) NodeFetchAll(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	return datasource.NodeFetchAll(ctx, f.ds, req, spannerPageSize)
}

// StoreNodeFetcher wraps Server to implement nodefetcher.NodeAllFetcher by querying Bigtable and SQL via V2NodeCore.
type StoreNodeFetcher struct {
	s *Server
}

// NewStoreNodeFetcher creates a new StoreNodeFetcher.
func NewStoreNodeFetcher(s *Server) *StoreNodeFetcher {
	return &StoreNodeFetcher{s: s}
}

// NodeFetchAll implements nodefetcher.NodeAllFetcher.
func (f *StoreNodeFetcher) NodeFetchAll(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	return V2NodeCoreFetchAll(ctx, f.s, req)
}

// V2NodeCoreFetchAll repeatedly calls s.V2NodeCore as long as NextToken is returned and merges results.
func V2NodeCoreFetchAll(ctx context.Context, s *Server, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	return fetcher.NodeFetchAllFunc(ctx, func(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
		return s.V2NodeCore(ctx, req)
	}, req)
}
