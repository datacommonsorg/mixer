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

// Package observation is for V2 observation API
package shared

import (
	"context"
	"fmt"
	"net/http"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/placein"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
)

const (
	LATEST = "LATEST"
)

// Num of concurrent series to read at a time. Set this to prevent OOM issue.
const MaxSeries = 5000

// Max number of nodes to be requested
const MaxNodes = 5000

// For mocking in tests.
var (
	getPlacesIn = placein.GetPlacesIn
	fetchRemote = fetchRemoteWrapper
)

func fetchRemoteWrapper(
	metadata *resource.Metadata,
	httpClient *http.Client,
	apiPath string,
	remoteReq *pbv2.NodeRequest,
) (*pbv2.NodeResponse, error) {
	remoteResp := &pbv2.NodeResponse{}
	err := util.FetchRemote(metadata, httpClient, apiPath, remoteReq, remoteResp)
	if err != nil {
		return nil, err
	}
	return remoteResp, nil
}

func storeFetchChildPlaces(
	ctx context.Context,
	store *store.Store,
	ancestor, childType string,
) (map[string][]string, error) {
	return getPlacesIn(ctx, store, []string{ancestor}, childType)
}

func remoteMixerFetchChildPlaces(
	metadata *resource.Metadata,
	httpClient *http.Client,
	ancestor, childType string,
) (*pbv2.NodeResponse, error) {
	remoteReq := &pbv2.NodeRequest{
		Nodes:    []string{ancestor},
		Property: fmt.Sprintf("<-containedInPlace+{typeOf:%s}", childType),
	}
	return fetchRemote(metadata, httpClient, "/v2/node", remoteReq)
}

// FetchChildPlaces fetches child places
func FetchChildPlaces(
	ctx context.Context,
	store *store.Store,
	metadata *resource.Metadata,
	httpClient *http.Client,
	remoteMixer, ancestor, childType string,
) ([]string, error) {
	errGroup, errCtx := errgroup.WithContext(ctx)

	storeResponseChan := make(chan map[string][]string, 1)
	remoteMixerResponseChan := make(chan *pbv2.NodeResponse, 1)

	errGroup.Go(func() error {
		storeResponse, err := storeFetchChildPlaces(errCtx, store, ancestor, childType)
		if err != nil {
			return err
		}
		storeResponseChan <- storeResponse
		return nil
	})

	if remoteMixer != "" {
		errGroup.Go(func() error {
			remoteMixerResponse, err := remoteMixerFetchChildPlaces(metadata, httpClient, ancestor, childType)
			if err != nil {
				return err
			}
			remoteMixerResponseChan <- remoteMixerResponse
			return nil
		})
	} else {
		remoteMixerResponseChan <- nil
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}
	close(storeResponseChan)
	close(remoteMixerResponseChan)

	childPlacesMap := <-storeResponseChan
	remoteResp := <-remoteMixerResponseChan

	childPlaces := childPlacesMap[ancestor]
	// V2 API should always ensure data merging.
	// Here needs to fetch both local PlacesIn and remote PlacesIn data
	if remoteResp != nil {
		if g, ok := remoteResp.Data[ancestor]; ok {
			for _, arc := range g.Arcs {
				for _, node := range arc.Nodes {
					childPlaces = append(childPlaces, node.Dcid)
				}
			}
		}
	}
	return childPlaces, nil
}
