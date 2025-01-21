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
package observation

import (
	"context"
	"net/http"

	"github.com/datacommonsorg/mixer/internal/merger"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/cache"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	v2facet "github.com/datacommonsorg/mixer/internal/server/v2/facet"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ObservationCore(
	ctx context.Context,
	store *store.Store,
	cachedata *cache.Cache,
	metadata *resource.Metadata,
	httpClient *http.Client,
	in *pbv2.ObservationRequest,
) (*pbv2.ObservationResponse, error) {
	// (TODO): The routing logic here is very rough. This needs more work.
	var queryDate, queryValue, queryVariable, queryEntity, queryFacet bool
	for _, item := range in.GetSelect() {
		if item == "date" {
			queryDate = true
		} else if item == "value" {
			queryValue = true
		} else if item == "variable" {
			queryVariable = true
		} else if item == "entity" {
			queryEntity = true
		} else if item == "facet" {
			queryFacet = true
		}
	}
	if !queryVariable || !queryEntity {
		return nil, status.Error(
			codes.InvalidArgument, "Must select 'variable' and 'entity'")
	}

	variable := in.GetVariable()
	entity := in.GetEntity()

	// Observation date and value query.
	if queryDate && queryValue {
		// Series.
		if len(variable.GetDcids()) > 0 && len(entity.GetDcids()) > 0 {
			return FetchDirect(
				ctx,
				store,
				cachedata.SQLProvenances(),
				variable.GetDcids(),
				entity.GetDcids(),
				in.GetDate(),
				in.GetFilter(),
			)
		}

		// Collection.
		if len(variable.GetDcids()) > 0 && entity.GetExpression() != "" {
			// Example of expression
			// "geoId/06<-containedInPlace+{typeOf: City}"
			expr := entity.GetExpression()
			containedInPlace, err := v2.ParseContainedInPlace(expr)
			if err != nil {
				return nil, err
			}
			return FetchContainedIn(
				ctx,
				store,
				metadata,
				cachedata.SQLProvenances(),
				httpClient,
				metadata.RemoteMixerDomain,
				variable.GetDcids(),
				containedInPlace.Ancestor,
				containedInPlace.ChildPlaceType,
				in.GetDate(),
				in.GetFilter(),
			)
		}

		// Derived series.
		if variable.GetFormula() != "" && len(entity.GetDcids()) > 0 {
			return DerivedSeries(
				ctx,
				store,
				variable.GetFormula(),
				entity.GetDcids(),
			)
		}
	}

	// Get facet information for <variable, entity> pair.
	if !queryDate && !queryValue && queryFacet {
		// Series
		if len(variable.GetDcids()) > 0 && len(entity.GetDcids()) > 0 {
			return v2facet.SeriesFacet(
				ctx,
				store,
				cachedata,
				variable.GetDcids(),
				entity.GetDcids(),
			)
		}
		// Collection
		if len(variable.GetDcids()) > 0 && entity.GetExpression() != "" {
			expr := entity.GetExpression()
			containedInPlace, err := v2.ParseContainedInPlace(expr)
			if err != nil {
				return nil, err
			}
			return v2facet.ContainedInFacet(
				ctx,
				store,
				cachedata,
				metadata,
				httpClient,
				metadata.RemoteMixerDomain,
				variable.GetDcids(),
				containedInPlace.Ancestor,
				containedInPlace.ChildPlaceType,
				in.GetDate(),
			)
		}
	}

	// Get existence of <variable, entity> pair.
	if !queryDate && !queryValue {
		if len(entity.GetDcids()) > 0 {
			if len(variable.GetDcids()) > 0 {
				// Have both entity.dcids and variable.dcids. Check existence cache.
				return Existence(
					ctx, store, cachedata, variable.GetDcids(), entity.GetDcids())
			}
			// TODO: Support appending entities from entity.expression
			// Only have entity.dcids, fetch variables for each entity.
			return Variable(ctx, store, entity.GetDcids())
		}
	}
	return &pbv2.ObservationResponse{}, nil
}

func ObservationInternal(
	ctx context.Context,
	store *store.Store,
	cachedata *cache.Cache,
	metadata *resource.Metadata,
	httpClient *http.Client,
	in *pbv2.ObservationRequest,
) (*pbv2.ObservationResponse, error) {
	errGroup, errCtx := errgroup.WithContext(ctx)
	localRespChan := make(chan *pbv2.ObservationResponse, 1)
	remoteRespChan := make(chan *pbv2.ObservationResponse, 1)

	errGroup.Go(func() error {
		localResp, err := ObservationCore(errCtx, store, cachedata, metadata, httpClient, in)
		if err != nil {
			return err
		}
		localRespChan <- localResp
		return nil
	})

	if metadata.RemoteMixerDomain != "" {
		errGroup.Go(func() error {
			remoteResp := &pbv2.ObservationResponse{}
			err := util.FetchRemote(metadata, httpClient, "/v2/observation", in, remoteResp)
			if err != nil {
				return err
			}
			remoteRespChan <- remoteResp
			return nil
		})
	} else {
		remoteRespChan <- nil
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}
	close(localRespChan) //
	close(remoteRespChan)
	localResp, remoteResp := <-localRespChan, <-remoteRespChan
	// The order of argument matters, localResp is prefered and will be put first
	// in the merged result.
	return merger.MergeObservation(localResp, remoteResp), nil
}
