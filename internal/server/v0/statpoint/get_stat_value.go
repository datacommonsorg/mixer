// Copyright 2020 Google LLC
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

package statpoint

import (
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/model"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetStatValue implements API for Mixer.GetStatValue.
func GetStatValue(ctx context.Context, in *pb.GetStatValueRequest, store *store.Store) (
	*pb.GetStatValueResponse, error) {
	place := in.GetPlace()
	statVar := in.GetStatVar()

	if place == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: place")
	}
	if statVar == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: stat_var")
	}
	date := in.GetDate()
	filterProp := &model.StatObsProp{
		MeasurementMethod: in.GetMeasurementMethod(),
		ObservationPeriod: in.GetObservationPeriod(),
		Unit:              in.GetUnit(),
		ScalingFactor:     in.GetScalingFactor(),
	}

	rowList, keyTokens := bigtable.BuildObsTimeSeriesKey([]string{place}, []string{statVar})
	var obsTimeSeries *model.ObsTimeSeries
	btData, err := stat.ReadStats(ctx, store.BtGroup, rowList, keyTokens)
	if err != nil {
		return nil, err
	}
	result := &pb.GetStatValueResponse{}
	obsTimeSeries = btData[place][statVar]
	if obsTimeSeries == nil {
		return result, nil
	}
	obsTimeSeries.SourceSeries = stat.FilterSeries(obsTimeSeries.SourceSeries, filterProp)
	value, err := stat.GetValueFromBestSource(obsTimeSeries, date)
	if err != nil {
		return result, nil
	}
	result.Value = value
	return result, nil
}
