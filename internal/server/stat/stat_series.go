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

	cbt "cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/model"
	"github.com/datacommonsorg/mixer/internal/server/place"
	"github.com/datacommonsorg/mixer/internal/server/ranking"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
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

	rowList, keyTokens := bigtable.BuildObsTimeSeriesKey([]string{place}, []string{statVar})
	btData, err := bigtable.ReadStats(ctx, store.BtGroup, rowList, keyTokens)
	if err != nil {
		return nil, err
	}
	series := btData[place][statVar].SourceSeries
	series = filterSeries(series, filterProp)
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
	result := &pb.GetStatAllResponse{
		PlaceData: make(map[string]*pb.PlaceStat),
	}
	for _, place := range places {
		result.PlaceData[place] = &pb.PlaceStat{
			StatVarData: make(map[string]*pb.ObsTimeSeries),
		}
		for _, statVar := range statVars {
			result.PlaceData[place].StatVarData[statVar] = nil
		}
	}

	rowList, keyTokens := bigtable.BuildObsTimeSeriesKey(places, statVars)
	cacheData, err := bigtable.ReadStatsPb(ctx, store.BtGroup, rowList, keyTokens)
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
	var rowList cbt.RowList
	var keyTokens map[string]*util.PlaceStatVar
	rowList, keyTokens = bigtable.BuildObsTimeSeriesKey(placeDcids, []string{statsVarDcid})

	tmp := map[string]*model.ObsTimeSeries{}
	cacheData, err := bigtable.ReadStats(ctx, store.BtGroup, rowList, keyTokens)
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
			if obsSeries.SourceSeries != nil && len(obsSeries.SourceSeries) > 0 {
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

// GetStatSetSeries implements API for Mixer.GetStatSetSeries.
func GetStatSetSeries(ctx context.Context, in *pb.GetStatSetSeriesRequest, store *store.Store) (
	*pb.GetStatSetSeriesResponse, error) {
	places := in.GetPlaces()
	statVars := in.GetStatVars()
	importName := in.GetImportName()
	if len(places) == 0 {
		return nil, status.Errorf(
			codes.InvalidArgument, "Missing required argument: places")
	}
	if len(statVars) == 0 {
		return nil, status.Errorf(
			codes.InvalidArgument, "Missing required argument: stat_vars")
	}

	// Initialize result with place and stat var dcids.
	result := &pb.GetStatSetSeriesResponse{
		Data: make(map[string]*pb.SeriesMap),
	}
	for _, place := range places {
		result.Data[place] = &pb.SeriesMap{
			Data: make(map[string]*pb.Series),
		}
		for _, statVar := range statVars {
			result.Data[place].Data[statVar] = nil
		}
	}
	// Read data from Cloud Bigtable.
	if store.BtGroup.Tables() != nil {
		rowList, keyTokens := bigtable.BuildObsTimeSeriesKey(places, statVars)

		// Read data from BigTable.
		cacheData, err := bigtable.ReadStatsPb(ctx, store.BtGroup, rowList, keyTokens)
		if err != nil {
			return nil, err
		}
		for place, placeData := range cacheData {
			for statVar, data := range placeData {
				if data != nil {
					series, _ := GetBestSeries(data, importName, false /* useLatest */)
					result.Data[place].Data[statVar] = series
				}
			}
		}
	}
	// Read data from in-memory cache (private data).
	// When there is data in both BigTable and private data. Prefer private data
	// as this instance is for a private DC.
	if !store.MemDb.IsEmpty() {
		for _, place := range places {
			for _, statVar := range statVars {
				series := store.MemDb.ReadSeries(statVar, place)
				if len(series) > 0 {
					// TODO: add ranking function for *pb.Series. Now only pick one series
					// from the private import.
					result.Data[place].Data[statVar] = series[0]
				}
			}
		}
	}
	return result, nil
}

// GetStatSetSeriesWithinPlace implements API for Mixer.GetStatSetSeriesWithinPlace.
func GetStatSetSeriesWithinPlace(
	ctx context.Context, in *pb.GetStatSetSeriesWithinPlaceRequest, store *store.Store) (
	*pb.GetStatSetSeriesResponse, error,
) {
	parentPlace := in.GetParentPlace()
	statVars := in.GetStatVars()
	childType := in.GetChildType()
	if parentPlace == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: parent_place")
	}
	if len(statVars) == 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: stat_vars")
	}
	if childType == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: child_type")
	}
	childPlaces, err := place.GetChildPlaces(ctx, store, parentPlace, childType)
	if err != nil {
		return nil, err
	}

	return GetStatSetSeries(
		ctx,
		&pb.GetStatSetSeriesRequest{
			Places:   childPlaces,
			StatVars: statVars,
		},
		store,
	)
}
