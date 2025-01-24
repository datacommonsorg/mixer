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
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"

	"golang.org/x/sync/errgroup"
)

// DataSources struct uses underlying data sources to respond to API requests.
type DataSources struct {
	sources []*datasource.DataSource
}

func NewDataSources(sources []*datasource.DataSource) *DataSources {
	return &DataSources{sources: sources}
}

func (ds *DataSources) Node(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	errGroup, errCtx := errgroup.WithContext(ctx)
	dsRespChan := []chan *pbv2.NodeResponse{}

	for _, source := range ds.sources {
		respChan := make(chan *pbv2.NodeResponse, 1)
		errGroup.Go(func() error {
			defer close(respChan)
			resp, err := (*source).Node(errCtx, in)
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
		respChan := make(chan *pbv2.ObservationResponse, 1)
		errGroup.Go(func() error {
			defer close(respChan)
			resp, err := (*source).Observation(errCtx, in)
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
	if len(ds.sources) == 0 {
		return nil, fmt.Errorf("no sources found")
	}
	// Returning only the first one right now.
	// TODO: Execute in parallel and returned merged response.
	return (*ds.sources[0]).NodeSearch(ctx, in)
}

func (ds *DataSources) Resolve(ctx context.Context, in *pbv2.ResolveRequest) (*pbv2.ResolveResponse, error) {
	if len(ds.sources) == 0 {
		return nil, fmt.Errorf("no sources found")
	}
	// Returning only the first one right now.
	// TODO: Execute in parallel and returned merged response.
	return (*ds.sources[0]).Resolve(ctx, in)
}
