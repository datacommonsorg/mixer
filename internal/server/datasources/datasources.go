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
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"

	"golang.org/x/sync/errgroup"
)

const (
	// Default page size for paginated responses.
	DefaultPageSize = 500
)

// DataSources struct uses underlying data sources to respond to API requests.
type DataSources struct {
	sources []*datasource.DataSource
}

func NewDataSources(sources []*datasource.DataSource) *DataSources {
	return &DataSources{sources: sources}
}

// GetSources returns the list of data source IDs.
func (ds *DataSources) GetSources() []string {
	sources := make([]string, 0, len(ds.sources))
	for _, source := range ds.sources {
		sources = append(sources, (*source).Id())
	}
	return sources
}

func (ds *DataSources) Node(ctx context.Context, in *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
	errGroup, errCtx := errgroup.WithContext(ctx)
	dsRespChan := []chan *pbv2.NodeResponse{}

	for _, source := range ds.sources {
		src := *source
		respChan := make(chan *pbv2.NodeResponse, 1)
		errGroup.Go(func() error {
			defer close(respChan)
			resp, err := src.Node(errCtx, in, pageSize)
			if err != nil {
				return err
			}
			respChan <- resp
			return nil
		})
		dsRespChan = append(dsRespChan, respChan)
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	allResp := []*pbv2.NodeResponse{}
	for _, respChan := range dsRespChan {
		allResp = append(allResp, <-respChan)
	}

	return merger.MergeMultiNode(allResp)
}

func (ds *DataSources) Observation(ctx context.Context, in *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
	errGroup, errCtx := errgroup.WithContext(ctx)
	dsRespChan := []chan *pbv2.ObservationResponse{}

	for _, source := range ds.sources {
		src := *source
		respChan := make(chan *pbv2.ObservationResponse, 1)
		errGroup.Go(func() error {
			defer close(respChan)
			resp, err := src.Observation(errCtx, in)
			if err != nil {
				return err
			}
			respChan <- resp
			return nil
		})
		dsRespChan = append(dsRespChan, respChan)
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	allResp := []*pbv2.ObservationResponse{}
	for _, respChan := range dsRespChan {
		allResp = append(allResp, <-respChan)
	}

	return merger.MergeMultiObservation(allResp), nil
}

func (ds *DataSources) NodeSearch(ctx context.Context, in *pbv2.NodeSearchRequest) (*pbv2.NodeSearchResponse, error) {
	errGroup, errCtx := errgroup.WithContext(ctx)
	dsRespChan := []chan *pbv2.NodeSearchResponse{}

	for _, source := range ds.sources {
		src := *source
		respChan := make(chan *pbv2.NodeSearchResponse, 1)
		errGroup.Go(func() error {
			defer close(respChan)
			resp, err := src.NodeSearch(errCtx, in)
			if err != nil {
				return err
			}
			respChan <- resp
			return nil
		})
		dsRespChan = append(dsRespChan, respChan)
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	allResp := []*pbv2.NodeSearchResponse{}
	for _, respChan := range dsRespChan {
		allResp = append(allResp, <-respChan)
	}

	return merger.MergeMultiNodeSearch(allResp)
}

func (ds *DataSources) Resolve(ctx context.Context, in *pbv2.ResolveRequest) (*pbv2.ResolveResponse, error) {
	errGroup, errCtx := errgroup.WithContext(ctx)
	dsRespChan := []chan *pbv2.ResolveResponse{}

	for _, source := range ds.sources {
		src := *source
		respChan := make(chan *pbv2.ResolveResponse, 1)
		errGroup.Go(func() error {
			defer close(respChan)
			resp, err := src.Resolve(errCtx, in)
			if err != nil {
				return err
			}
			respChan <- resp
			return nil
		})
		dsRespChan = append(dsRespChan, respChan)
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	allResp := []*pbv2.ResolveResponse{}
	for _, respChan := range dsRespChan {
		allResp = append(allResp, <-respChan)
	}

	return merger.MergeMultiResolve(allResp), nil
}
