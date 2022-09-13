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

// API Implementation for /v1/bulk/observations/series/derived

// Package observations contain code for observations.
package observations

import (
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/store"
)

// DerivedSeries implements API for Mixer.DerivedObservationsSeries.
func DerivedSeries(
	ctx context.Context,
	in *pb.DerivedObservationsSeriesRequest,
	store *store.Store,
) (*pb.DerivedObservationsSeriesResponse, error) {
	resp := &pb.DerivedObservationsSeriesResponse{}
	entity := in.GetEntity()

	// Parse the formula to extract all the variables, used for reading data from BT.
	calculator, err := newCalculator(in.GetFormula())
	if err != nil {
		return resp, err
	}
	statVars := calculator.statVars()

	// Read data from BT.
	btData, err := stat.ReadStatsPb(
		ctx, store.BtGroup, []string{entity}, statVars)
	if err != nil {
		return resp, err
	}
	entityData, ok := btData[entity]
	if !ok {
		return resp, err
	}

	// Calculate.
	result, err := calculator.calculate(entityData)
	if err != nil {
		return resp, err
	}
	for _, p := range result.points {
		resp.Observations = append(resp.Observations, &pb.PointStat{
			Date:  p.GetDate(),
			Value: p.GetValue(),
		})
	}

	return resp, nil
}
