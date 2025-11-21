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
	"github.com/datacommonsorg/mixer/internal/server/v2/shared"
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
) (*pbv2.ObservationResponse, shared.QueryType, error) {
	// (TODO): The routing logic here is very rough. This needs more work.
	var queryDate, queryValue, queryVariable, queryEntity, queryFacet bool
	for _, item := range in.GetSelect() {
		switch item {
		case "date":
			queryDate = true
		case "value":
			queryValue = true
		case "variable":
			queryVariable = true
		case "entity":
			queryEntity = true
		case "facet":
			queryFacet = true
		}
	}
	if !queryVariable || !queryEntity {
		return nil, "", status.Error(
			codes.InvalidArgument, "Must select 'variable' and 'entity'")
	}

	variable := in.GetVariable()
	entity := in.GetEntity()

	// Observation date and value query.
	if queryDate && queryValue {
		// Series.
		if len(variable.GetDcids()) > 0 && len(entity.GetDcids()) > 0 {
			result, err := FetchDirect(
				ctx,
				store,
				cachedata.SQLProvenances(ctx),
				variable.GetDcids(),
				entity.GetDcids(),
				in.GetDate(),
				in.GetFilter(),
			)

			return result, shared.QueryTypeValue, err
		}

		// Collection.
		if len(variable.GetDcids()) > 0 && entity.GetExpression() != "" {
			// Example of expression
			// "geoId/06<-containedInPlace+{typeOf: City}"
			expr := entity.GetExpression()
			containedInPlace, err := v2.ParseContainedInPlace(expr)
			if err != nil {
				return nil, "", err
			}

			res, err := FetchContainedIn(
				ctx,
				store,
				metadata,
				cachedata.SQLProvenances(ctx),
				httpClient,
				metadata.RemoteMixerDomain,
				variable.GetDcids(),
				containedInPlace.Ancestor,
				containedInPlace.ChildPlaceType,
				in.GetDate(),
				in.GetFilter(),
			)
			return res, shared.QueryTypeValue, err
		}

		// Derived series.
		if variable.GetFormula() != "" && len(entity.GetDcids()) > 0 {
			res, err := DerivedSeries(
				ctx,
				store,
				variable.GetFormula(),
				entity.GetDcids(),
			)

			return res, shared.QueryTypeDerived, err
		}
	}

	// Get facet information for <variable, entity> pair.
	if !queryDate && !queryValue && queryFacet {
		// Series
		if len(variable.GetDcids()) > 0 && len(entity.GetDcids()) > 0 {
			res, err := v2facet.SeriesFacet(
				ctx,
				store,
				cachedata,
				variable.GetDcids(),
				entity.GetDcids(),
			)

			return res, shared.QueryTypeFacet, err
		}
		// Collection
		if len(variable.GetDcids()) > 0 && entity.GetExpression() != "" {
			expr := entity.GetExpression()
			containedInPlace, err := v2.ParseContainedInPlace(expr)
			if err != nil {
				return nil, "", err
			}
			res, err := v2facet.ContainedInFacet(
				ctx,
				store,
				cachedata,
				metadata,
				cachedata.SQLProvenances(ctx),
				httpClient,
				metadata.RemoteMixerDomain,
				variable.GetDcids(),
				containedInPlace.Ancestor,
				containedInPlace.ChildPlaceType,
				in.GetDate(),
			)

			return res, shared.QueryTypeFacet, err
		}
	}

	// Get existence of <variable, entity> pair.
	if !queryDate && !queryValue {
		if len(entity.GetDcids()) > 0 {
			if len(variable.GetDcids()) > 0 {
				// Have both entity.dcids and variable.dcids. Check existence cache.
				res, err := Existence(
					ctx, store, cachedata, variable.GetDcids(), entity.GetDcids())
				return res, shared.QueryTypeExistenceByVar, err
			}
			// TODO: Support appending entities from entity.expression
			// Only have entity.dcids, fetch variables for each entity.
			res, err := Variable(ctx, store, entity.GetDcids())

			return res, shared.QueryTypeExistenceByEntity, err
		}
	}
	return &pbv2.ObservationResponse{}, "", nil
}

func ObservationInternal(
	ctx context.Context,
	store *store.Store,
	cachedata *cache.Cache,
	metadata *resource.Metadata,
	httpClient *http.Client,
	in *pbv2.ObservationRequest,
	surface string,
) (*pbv2.ObservationResponse, shared.QueryType, error) {
	errGroup, errCtx := errgroup.WithContext(ctx)
	localRespChan := make(chan *pbv2.ObservationResponse, 1)
	remoteRespChan := make(chan *pbv2.ObservationResponse, 1)
	queryTypeChan := make(chan shared.QueryType, 1)

	errGroup.Go(func() error {
		localResp, queryType, err := ObservationCore(errCtx, store, cachedata, metadata, httpClient, in)
		if err != nil {
			return err
		}
		localRespChan <- localResp
		// We only need to get the query type from the initial observation because
		// it's based on the query parameters and will be the same in remote and local mixers.
		queryTypeChan <- queryType
		return nil
	})

	if metadata.RemoteMixerDomain != "" {
		errGroup.Go(func() error {
			remoteResp := &pbv2.ObservationResponse{}
			err := util.FetchRemote(
				metadata,
				httpClient,
				"/v2/observation",
				in,
				remoteResp,
				surface,
			)
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
		return nil, "", err
	}
	close(localRespChan) //
	close(remoteRespChan)
	close(queryTypeChan)
	localResp, remoteResp, queryType := <-localRespChan, <-remoteRespChan, <-queryTypeChan
	// The order of argument matters, localResp is prefered and will be put first
	// in the merged result.
	return merger.MergeObservation(localResp, remoteResp), queryType, nil
}
