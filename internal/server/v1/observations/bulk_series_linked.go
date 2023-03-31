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

// API Implementation for /v1/bulk/observations/point/linked

package observations

import (
	"context"

	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/placein"
	"github.com/datacommonsorg/mixer/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BulkSeriesLinked implements API for Mixer.BulkObservationsSeriesLinked.
func BulkSeriesLinked(
	ctx context.Context,
	in *pbv1.BulkObservationsSeriesLinkedRequest,
	store *store.Store,
) (*pbv1.BulkObservationsSeriesResponse, error) {
	entityType := in.GetEntityType()
	linkedEntity := in.GetLinkedEntity()
	linkedProperty := in.GetLinkedProperty()
	variables := in.GetVariables()
	allFacets := in.GetAllFacets()
	if linkedEntity == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"missing required argument: linked_entity")
	}
	if linkedProperty != "containedInPlace" {
		return nil, status.Errorf(codes.InvalidArgument,
			"linked_property can only be 'containedInPlace'")
	}
	if len(variables) == 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"missing required argument: variables")
	}
	if entityType == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"missing required argument: entity_type")
	}

	// TODO(shifucun): use V1 API /v1/bulk/property/out/values/linked here
	childPlacesMap, err := placein.GetPlacesIn(ctx, store, []string{linkedEntity}, entityType)
	if err != nil {
		return nil, err
	}
	childPlaces := childPlacesMap[linkedEntity]
	req := &pbv1.BulkObservationsSeriesRequest{
		Entities:  childPlaces,
		Variables: variables,
		AllFacets: allFacets,
	}
	return BulkSeries(ctx, req, store)
}
