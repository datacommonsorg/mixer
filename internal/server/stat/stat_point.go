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
	"log"
	"sort"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/model"
	"github.com/datacommonsorg/mixer/internal/server/placein"
	"github.com/datacommonsorg/mixer/internal/server/ranking"
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
	btData, err := ReadStats(ctx, store.BtGroup, rowList, keyTokens)
	if err != nil {
		return nil, err
	}
	result := &pb.GetStatValueResponse{}
	obsTimeSeries = btData[place][statVar]
	if obsTimeSeries == nil {
		return result, nil
	}
	obsTimeSeries.SourceSeries = filterSeries(obsTimeSeries.SourceSeries, filterProp)
	value, err := getValueFromBestSource(obsTimeSeries, date)
	if err != nil {
		log.Println(err)
		return result, nil
	}
	result.Value = value
	return result, nil
}

func getStatSet(
	ctx context.Context, store *store.Store, places []string, statVars []string, date string) (
	*pb.GetStatSetResponse, error) {
	// Initialize result with stat vars and place dcids.
	ts := time.Now()
	result := &pb.GetStatSetResponse{
		Data:     make(map[string]*pb.PlacePointStat),
		Metadata: make(map[uint32]*pb.StatMetadata),
	}
	for _, statVar := range statVars {
		result.Data[statVar] = &pb.PlacePointStat{
			Stat: make(map[string]*pb.PointStat),
		}
		for _, place := range places {
			result.Data[statVar].Stat[place] = nil
		}
	}

	rowList, keyTokens := bigtable.BuildObsTimeSeriesKey(places, statVars)
	cacheData, err := ReadStatsPb(ctx, store.BtGroup, rowList, keyTokens)
	if err != nil {
		return nil, err
	}
	for _, place := range places {
		placeData, ok := cacheData[place]
		if !ok {
			continue
		}
		for _, statVar := range statVars {
			data, ok := placeData[statVar]
			if !ok || data == nil {
				continue
			}
			stat, metaData := getValueFromBestSourcePb(data, date)
			if stat == nil {
				continue
			}
			metaHash := getMetadataHash(metaData)
			stat.MetaHash = metaHash
			result.Data[statVar].Stat[place] = stat
			result.Metadata[metaHash] = metaData
		}
	}
	log.Printf("getStatSet() completed for %d places, %d stat vars, in %s seconds",
		len(places), len(statVars), time.Since(ts))
	return result, nil
}

func getStatSetAll(
	ctx context.Context, store *store.Store, places []string, statVars []string, date string) (
	*pb.GetStatSetAllResponse, error,
) {
	ts := time.Now()
	rowList, keyTokens := bigtable.BuildObsTimeSeriesKey(places, statVars)
	cacheData, err := ReadStatsPb(ctx, store.BtGroup, rowList, keyTokens)
	if err != nil {
		return nil, err
	}
	// Use a temporary result to hold statVar->source->(place,value) data, then
	// convert to final result
	tmpResult := map[string]map[uint32]*pb.PlacePointStat{}
	// Initialize result with stat vars and place dcids.
	result := &pb.GetStatSetAllResponse{
		Data:     make(map[string]*pb.PlacePointStatAll),
		Metadata: make(map[uint32]*pb.StatMetadata),
	}

	// Populate tmp result
	for _, place := range places {
		placeData, ok := cacheData[place]
		if !ok {
			continue
		}
		for _, statVar := range statVars {
			ObsTimeSeries, ok := placeData[statVar]
			if !ok || ObsTimeSeries == nil {
				continue
			}
			if _, ok := tmpResult[statVar]; !ok {
				tmpResult[statVar] = map[uint32]*pb.PlacePointStat{}
			}
			for _, series := range ObsTimeSeries.SourceSeries {
				metadata := &pb.StatMetadata{
					ImportName:        series.ImportName,
					ProvenanceUrl:     series.ProvenanceUrl,
					MeasurementMethod: series.MeasurementMethod,
					ObservationPeriod: series.ObservationPeriod,
					ScalingFactor:     series.ScalingFactor,
					Unit:              series.Unit,
				}
				metaHash := getMetadataHash(metadata)
				if _, ok := tmpResult[statVar][metaHash]; !ok {
					tmpResult[statVar][metaHash] = &pb.PlacePointStat{
						Stat: map[string]*pb.PointStat{},
					}
				}
				// Date is given
				if date != "" {
					if value, ok := series.Val[date]; ok {
						tmpResult[statVar][metaHash].Stat[place] = &pb.PointStat{
							Date:  date,
							Value: value,
						}
					}
				} else {
					// Date is not given, find the latest value
					latestDate := ""
					var ps *pb.PointStat
					for date, value := range series.Val {
						if date > latestDate {
							latestDate = date
							ps = &pb.PointStat{
								Date:  date,
								Value: value,
							}
						}
					}
					tmpResult[statVar][metaHash].Stat[place] = ps
					tmpResult[statVar][metaHash].MetaHash = metaHash
				}
				result.Metadata[metaHash] = metadata
			}
		}
	}

	// Convert tmp result to result
	for _, statVar := range statVars {
		result.Data[statVar] = &pb.PlacePointStatAll{
			StatList: []*pb.PlacePointStat{},
		}
	}
	for statVar, sourceData := range tmpResult {
		metaHashList := []uint32{}
		for metaHash := range sourceData {
			metaHashList = append(metaHashList, metaHash)
		}
		sort.Slice(metaHashList, func(i, j int) bool { return metaHashList[i] < metaHashList[j] })
		for _, metaHash := range metaHashList {
			result.Data[statVar].StatList = append(
				result.Data[statVar].StatList,
				sourceData[metaHash],
			)
		}
	}

	log.Printf("getStatSetAll() completed for %d places, %d stat vars, in %s seconds",
		len(places), len(statVars), time.Since(ts))
	return result, nil
}

// GetStatSet implements API for Mixer.GetStatSet.
func GetStatSet(ctx context.Context, in *pb.GetStatSetRequest, store *store.Store) (
	*pb.GetStatSetResponse, error) {
	// Attach a hash store to the context
	places := in.GetPlaces()
	statVars := in.GetStatVars()
	date := in.GetDate()

	if len(places) == 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: places")
	}
	if len(statVars) == 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: stat_vars")
	}
	return getStatSet(ctx, store, places, statVars, date)
}

// GetStatSetWithinPlace implements API for Mixer.GetStatSetWithinPlace.
func GetStatSetWithinPlace(
	ctx context.Context, in *pb.GetStatSetWithinPlaceRequest, store *store.Store) (
	*pb.GetStatSetResponse, error,
) {
	parentPlace := in.GetParentPlace()
	statVars := in.GetStatVars()
	childType := in.GetChildType()
	date := in.GetDate()

	log.Printf(
		"GetStatSetWithinPlace: parentPlace: %s, statVars: %v, childType: %s, date: %s",
		parentPlace,
		statVars,
		childType,
		date,
	)
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
	dateKey := date
	if date == "" {
		dateKey = "LATEST"
	}

	// Pre-populate result
	result := &pb.GetStatSetResponse{
		Data:     make(map[string]*pb.PlacePointStat),
		Metadata: make(map[uint32]*pb.StatMetadata),
	}
	for _, statVar := range statVars {
		result.Data[statVar] = &pb.PlacePointStat{
			Stat: make(map[string]*pb.PointStat),
		}
	}

	// Read from cache directly
	rowList, keyTokens := bigtable.BuildObsCollectionKey(parentPlace, childType, dateKey, statVars)
	cacheData, err := ReadStatCollection(ctx, store.BtGroup, rowList, keyTokens)
	if err != nil {
		return nil, err
	}

	gotResult := false
	for _, statVar := range statVars {
		data, ok := cacheData[statVar]
		if !ok || data == nil {
			continue
		}
		gotResult = true
		cohorts := data.SourceCohorts
		// Sort cohort first, so the preferred source is populated first.
		sort.Sort(ranking.CohortByRank(cohorts))
		// update when there is a later data.
		for _, cohort := range cohorts {
			metaData := &pb.StatMetadata{
				MeasurementMethod: cohort.MeasurementMethod,
				ObservationPeriod: cohort.ObservationPeriod,
				ProvenanceUrl:     cohort.ProvenanceUrl,
				ScalingFactor:     cohort.ScalingFactor,
				ImportName:        cohort.ImportName,
				Unit:              cohort.Unit,
			}
			for place, val := range cohort.Val {
				// This works when date is set. The result will be populated in first
				// for loop only.
				if _, ok := result.Data[statVar].Stat[place]; !ok {
					usedDate := date
					if usedDate == "" {
						usedDate = cohort.PlaceToLatestDate[place]
					}
					metaHash := getMetadataHash(metaData)
					result.Data[statVar].Stat[place] = &pb.PointStat{
						Date:     usedDate,
						Value:    val,
						MetaHash: metaHash,
					}
					result.Metadata[metaHash] = metaData
				}
			}
		}
	}
	// Check if need to read from memory database.
	statVarInMemDb := false
	for _, statVar := range statVars {
		if store.MemDb.HasStatVar(statVar) {
			statVarInMemDb = true
			break
		}
	}
	// Need to fetch child places if need to read data from memory database or
	// from per place,statvar bigtable.
	var childPlaces []string
	if !gotResult || statVarInMemDb {
		childPlaces, err = placein.GetChildPlaces(ctx, store, parentPlace, childType)
		if err != nil {
			return nil, err
		}
	}
	// No data found from cache, fetch stat series for each place separately.
	if !gotResult {
		result, err = getStatSet(ctx, store, childPlaces, statVars, date)
		if err != nil {
			return nil, err
		}
	}

	// Merge data from in-memory database.
	if statVarInMemDb {
		for _, statVar := range statVars {
			for _, place := range childPlaces {
				pointValue, metaData := store.MemDb.ReadPointValue(statVar, place, date)
				// Override public data from private import
				if pointValue != nil {
					metaHash := getMetadataHash(metaData)
					pointValue.MetaHash = metaHash
					result.Data[statVar].Stat[place] = pointValue
					result.Metadata[metaHash] = metaData
				}
			}
		}
	}

	return result, nil
}

// GetStatSetWithinPlaceAll implements API for Mixer.GetStatSetWithinPlaceAll.
func GetStatSetWithinPlaceAll(
	ctx context.Context, in *pb.GetStatSetWithinPlaceRequest, store *store.Store) (
	*pb.GetStatSetAllResponse, error,
) {
	parentPlace := in.GetParentPlace()
	statVars := in.GetStatVars()
	childType := in.GetChildType()
	date := in.GetDate()
	log.Printf(
		"GetStatSetWithinPlaceAll: parentPlace: %s, statVars: %v, childType: %s, date: %s",
		parentPlace,
		statVars,
		childType,
		date,
	)
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
	dateKey := date
	if date == "" {
		dateKey = "LATEST"
	}

	// Pre-populate result
	result := &pb.GetStatSetAllResponse{
		Data:     make(map[string]*pb.PlacePointStatAll),
		Metadata: make(map[uint32]*pb.StatMetadata),
	}
	for _, statVar := range statVars {
		result.Data[statVar] = &pb.PlacePointStatAll{
			StatList: []*pb.PlacePointStat{},
		}
	}

	// Read from cache directly
	rowList, keyTokens := bigtable.BuildObsCollectionKey(parentPlace, childType, dateKey, statVars)
	cacheData, err := ReadStatCollection(ctx, store.BtGroup, rowList, keyTokens)
	if err != nil {
		return nil, err
	}

	gotResult := false
	for _, statVar := range statVars {
		data, ok := cacheData[statVar]
		if !ok || data == nil {
			continue
		}
		gotResult = true
		sort.Sort(ranking.CohortByRank(data.SourceCohorts))
		for _, cohort := range data.SourceCohorts {
			// The cohort is from the same source.
			metaData := &pb.StatMetadata{
				MeasurementMethod: cohort.MeasurementMethod,
				ObservationPeriod: cohort.ObservationPeriod,
				ProvenanceUrl:     cohort.ProvenanceUrl,
				ScalingFactor:     cohort.ScalingFactor,
				ImportName:        cohort.ImportName,
				Unit:              cohort.Unit,
			}
			metaHash := getMetadataHash(metaData)
			pointStat := &pb.PlacePointStat{
				MetaHash: metaHash,
				Stat:     map[string]*pb.PointStat{},
			}
			for place, val := range cohort.Val {
				usedDate := date
				if usedDate == "" {
					usedDate = cohort.PlaceToLatestDate[place]
				}
				pointStat.Stat[place] = &pb.PointStat{
					Date:  usedDate,
					Value: val,
				}
			}
			result.Data[statVar].StatList = append(result.Data[statVar].StatList, pointStat)
			result.Metadata[metaHash] = metaData
		}
	}
	// Check if need to read from memory database.
	statVarInMemDb := false
	for _, statVar := range statVars {
		if store.MemDb.HasStatVar(statVar) {
			statVarInMemDb = true
			break
		}
	}
	// Need to fetch child places if to read data from memory database or
	// from per place,statvar bigtable.
	var childPlaces []string
	if !gotResult || statVarInMemDb {
		childPlaces, err = placein.GetChildPlaces(ctx, store, parentPlace, childType)
		if err != nil {
			return nil, err
		}
	}
	// No data found from cache, fetch stat series for each place separately.
	if !gotResult {
		result, err = getStatSetAll(ctx, store, childPlaces, statVars, date)
		if err != nil {
			return nil, err
		}
	}

	// Add data from in-memory database.
	if statVarInMemDb {
		for _, statVar := range statVars {
			stat := &pb.PlacePointStat{
				Stat: make(map[string]*pb.PointStat),
			}
			for i, place := range childPlaces {
				pointValue, metaData := store.MemDb.ReadPointValue(statVar, place, date)
				var metaHash uint32
				if pointValue != nil {
					if i == 0 {
						metaHash = getMetadataHash(metaData)
						result.Metadata[metaHash] = metaData
						stat.MetaHash = metaHash
					}
					stat.Stat[place] = pointValue
				}
			}
			result.Data[statVar].StatList = append(result.Data[statVar].StatList, stat)
		}
	}
	return result, nil
}
