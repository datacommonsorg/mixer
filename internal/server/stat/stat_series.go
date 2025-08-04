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

package stat

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/model"
	"github.com/datacommonsorg/mixer/internal/server/ranking"
	"github.com/datacommonsorg/mixer/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func logIfDurationTooLong(t time.Time, thresholdSec float32, msg string) {
	duration := time.Since(t).Seconds()
	if duration > float64(thresholdSec) {
		log.Printf("%f seconds:\n%s", duration, msg)
	}
}

// GetStatSeries implements API for Mixer.GetStatSeries.
// TODO(shifucun): consilidate and dedup the logic among these similar APIs.
func GetStatSeries(
	ctx context.Context, in *pb.GetStatSeriesRequest, store *store.Store) (
	*pb.GetStatSeriesResponse, error) {
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
	filterProp := &model.StatObsProp{
		MeasurementMethod: in.GetMeasurementMethod(),
		ObservationPeriod: in.GetObservationPeriod(),
		Unit:              in.GetUnit(),
		ScalingFactor:     in.GetScalingFactor(),
	}

	btData, err := ReadStats(ctx, store.BtGroup, []string{place}, []string{statVar})
	if err != nil {
		return nil, err
	}
	series := btData[place][statVar].SourceSeries
	series = FilterSeries(series, filterProp)
	sort.Sort(ranking.ByRank(series))
	resp := pb.GetStatSeriesResponse{Series: map[string]float64{}}
	if len(series) > 0 {
		resp.Series = series[0].Val
	}
	return &resp, nil
}

// GetStatAll implements API for Mixer.GetStatAll.
func GetStatAll(ctx context.Context, in *pb.GetStatAllRequest, store *store.Store) (
	*pb.GetStatAllResponse, error) {

	places := in.GetPlaces()
	statVars := in.GetStatVars()
	if len(places) == 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: place")
	}
	if len(statVars) == 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: stat_var")
	}

	// Initialize result with place and stat var dcids.
	result := &pb.GetStatAllResponse{PlaceData: make(map[string]*pb.PlaceStat)}
	for _, place := range places {
		result.PlaceData[place] = &pb.PlaceStat{
			StatVarData: make(map[string]*pb.ObsTimeSeries),
		}
		for _, statVar := range statVars {
			result.PlaceData[place].StatVarData[statVar] = nil
		}
	}

	cacheData, err := ReadStatsPb(ctx, store.BtGroup, places, statVars)
	if err != nil {
		return nil, err
	}
	for place, placeData := range cacheData {
		for statVar, data := range placeData {
			if data != nil && data.SourceSeries != nil {
				sort.Sort(ranking.SeriesByRank(data.SourceSeries))
			}
			result.PlaceData[place].StatVarData[statVar] = data
		}
	}
	return result, nil
}

// GetStats implements API for Mixer.GetStats.
func GetStats(ctx context.Context, in *pb.GetStatsRequest, store *store.Store) (
	*pb.GetStatsResponse, error) {
	ts := time.Now()
	placeDcids := in.GetPlace()
	statsVarDcid := in.GetStatsVar()
	defer logIfDurationTooLong(
		ts,
		30,
		fmt.Sprintf(
			"GetStats(): placeDcids: %s, statsVarDcid: %v", placeDcids, statsVarDcid),
	)

	if len(placeDcids) == 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: place")
	}
	if statsVarDcid == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: stat_var")
	}
	filterProp := &model.StatObsProp{
		MeasurementMethod: in.GetMeasurementMethod(),
		ObservationPeriod: in.GetObservationPeriod(),
		Unit:              in.GetUnit(),
		ScalingFactor:     in.GetScalingFactor(),
	}
	tmp := map[string]*model.ObsTimeSeries{}
	cacheData, err := ReadStats(ctx, store.BtGroup, placeDcids, []string{statsVarDcid})
	if err != nil {
		return nil, err
	}
	for place := range cacheData {
		tmp[place] = cacheData[place][statsVarDcid]
	}

	// Fill missing place data and result result
	for _, dcid := range placeDcids {
		if _, ok := tmp[dcid]; !ok {
			tmp[dcid] = nil
		}
	}
	result := map[string]*model.GetStatsResponse{}
	for place, obsSeries := range tmp {
		if obsSeries != nil {
			FilterAndRank(obsSeries, filterProp)
			result[place] = &model.GetStatsResponse{
				PlaceName: obsSeries.PlaceName,
			}
			if len(obsSeries.SourceSeries) > 0 {
				result[place].Data = obsSeries.SourceSeries[0].Val
				result[place].ProvenanceURL = obsSeries.SourceSeries[0].ProvenanceURL
			}
		}

	}
	jsonRaw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return &pb.GetStatsResponse{Payload: string(jsonRaw)}, nil
}
