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

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/cache"
	"github.com/datacommonsorg/mixer/internal/store"
)

// CalculateObservationResponses returns a list of ObservationResponses based
// on StatisticalCalculation formulas for the missing data in an input
// ObservationResponse.
func CalculateObservationResponses(
	ctx context.Context,
	store *store.Store,
	inputResp *pbv2.ObservationResponse,
	cachedata *cache.Cache,
) []*pbv2.ObservationResponse {
	calculatedResponses := []*pbv2.ObservationResponse{}
	for variable, variableObservation := range inputResp.ByVariable {
		formulas, ok := cachedata.SVFormula()[variable]
		if !ok {
			continue
		}
		for entity, entityObservation := range variableObservation.ByEntity {
			// Response already contains data.
			if len(entityObservation.OrderedFacets) != 0 {
				continue
			}
			// Use first formula that returns data.
			for _, formula := range formulas {
				derivedSeries, err := DerivedSeries(
					ctx,
					store,
					formula,
					[]string{entity},
				)
				// Missing input data.
				if err != nil {
					continue
				}
				// Successful calculation.
				bv := derivedSeries.ByVariable
				if len(bv[formula].ByEntity[entity].OrderedFacets) > 0 {
					// Re-label formula response with the outputProperty and delete
					// formula.
					bv[variable] = bv[formula]
					delete(bv, formula)
					calculatedResponses = append(calculatedResponses, derivedSeries)
					continue
				}
			}
		}
	}
	return calculatedResponses
}
