// Copyright 2023 Google LLC
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

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"

	v1o "github.com/datacommonsorg/mixer/internal/server/v1/observations"
	"github.com/datacommonsorg/mixer/internal/store"
)

// DerivedSeries computes derived series.
func DerivedSeries(
	ctx context.Context,
	store *store.Store,
	variable string,
	entities []string,
) (*pbv2.ObservationResponse, error) {
	resp := &pbv2.ObservationResponse{
		ByVariable: map[string]*pbv2.VariableObservation{
			variable: {
				ByEntity: map[string]*pbv2.EntityObservation{},
			},
		},
		Facets: map[string]*pb.Facet{},
	}

	for _, entity := range entities {
		data, err := v1o.DerivedSeries(ctx,
			&pbv1.DerivedObservationsSeriesRequest{
				Entity:  entity,
				Formula: variable,
			}, store)
		if err != nil {
			return nil, err
		}
		observations := data.GetObservations()
		earliestDate := ""
		latestDate := ""
		obsCount := int32(len(observations))
		if obsCount > 0 {
			earliestDate = observations[0].Date
			latestDate = observations[len(observations)-1].Date
		}
		resp.ByVariable[variable].ByEntity[entity] = &pbv2.EntityObservation{
			OrderedFacets: []*pbv2.FacetObservation{
				{
					Observations: data.GetObservations(),
					ObsCount:     obsCount,
					EarliestDate: earliestDate,
					LatestDate:   latestDate,
				},
			},
		}
	}
	return resp, nil
}
