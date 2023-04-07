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

// Package observationmetric is for V2 observation Metric API
package observationmetric

import (
	"context"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/statvar"
	"github.com/datacommonsorg/mixer/internal/store"
)

func VariableMetric(
	ctx context.Context,
	store *store.Store,
	entities []string,
) (*pbv2.ObservationResponse, error) {
	entityToStatVars, err := statvar.GetEntityStatVarsHelper(ctx, entities, store)
	if err != nil {
		return nil, err
	}
	resp := &pbv2.ObservationResponse{
		ObservationsByVariable: map[string]*pbv2.VariableObservation{},
	}
	for _, entity := range entities {
		if statVars, ok := entityToStatVars[entity]; ok {
			for _, variable := range statVars.StatVars {
				obsByVar := resp.ObservationsByVariable // Short alias
				if _, ok := obsByVar[variable]; !ok {
					obsByVar[variable] = &pbv2.VariableObservation{
						ObservationsByEntity: map[string]*pbv2.EntityObservation{},
					}
				}
				obsByVar[variable].ObservationsByEntity[entity] = &pbv2.EntityObservation{}
			}
		}
	}
	return resp, nil
}
