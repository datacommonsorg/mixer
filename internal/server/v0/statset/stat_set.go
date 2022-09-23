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

package statset

import (
	"context"
	"log"
	"sort"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/placein"
	"github.com/datacommonsorg/mixer/internal/server/ranking"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func getStatSet(
	ctx context.Context, store *store.Store, places []string, statVars []string, date string) (
	*pb.GetStatSetResponse, error) {
	// Initialize result with stat vars and place dcids.
	ts := time.Now()
	result := &pb.GetStatSetResponse{
		Data:     make(map[string]*pb.PlacePointStat),
		Metadata: make(map[string]*pb.StatMetadata),
	}
	for _, statVar := range statVars {
		result.Data[statVar] = &pb.PlacePointStat{
			Stat: make(map[string]*pb.PointStat),
		}
		for _, place := range places {
			result.Data[statVar].Stat[place] = nil
		}
	}
	cacheData, err := stat.ReadStatsPb(ctx, store.BtGroup, places, statVars)
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
			stat, metaData := stat.GetValueFromBestSourcePb(data, date)
			if stat == nil {
				continue
			}
			metaHash := util.GetMetadataHash(metaData)
			stat.MetaHash = metaHash
			result.Data[statVar].Stat[place] = stat
			result.Metadata[metaHash] = metaData
		}
	}
	log.Printf("getStatSet() completed for %d places, %d stat vars, in %s",
		len(places), len(statVars), time.Since(ts))
	return result, nil
}

func getStatSetAll(
	ctx context.Context, store *store.Store, places []string, statVars []string, date string) (
	*pb.GetStatSetAllResponse, error,
) {
	ts := time.Now()
	cacheData, err := stat.ReadStatsPb(ctx, store.BtGroup, places, statVars)
	if err != nil {
		return nil, err
	}
	// Use a temporary result to hold statVar->source->(place,value) data, then
	// convert to final result
	tmpResult := map[string]map[string]*pb.PlacePointStat{}
	// Initialize result with stat vars and place dcids.
	result := &pb.GetStatSetAllResponse{
		Data:     make(map[string]*pb.PlacePointStatAll),
		Metadata: make(map[string]*pb.StatMetadata),
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
				tmpResult[statVar] = map[string]*pb.PlacePointStat{}
			}
			for _, series := range ObsTimeSeries.SourceSeries {
				metadata := stat.GetMetadata(series)
				metaHash := util.GetMetadataHash(metadata)
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
							Value: proto.Float64(value),
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
								Value: proto.Float64(value),
							}
						}
					}
					tmpResult[statVar][metaHash].Stat[place] = ps
				}
				tmpResult[statVar][metaHash].MetaHash = metaHash
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
		metaHashList := []string{}
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

	log.Printf("getStatSetAll() completed for %d places, %d stat vars, in %s",
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
		Metadata: make(map[string]*pb.StatMetadata),
	}
	for _, statVar := range statVars {
		result.Data[statVar] = &pb.PlacePointStat{
			Stat: make(map[string]*pb.PointStat),
		}
	}

	// Read from cache directly
	cacheData, err := stat.ReadStatCollection(
		ctx, store.BtGroup, bigtable.BtObsCollection,
		parentPlace, childType, statVars, dateKey,
	)
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
		for _, cohort := range cohorts {
			metaData := stat.GetMetadata(cohort)
			metaHash := util.GetMetadataHash(metaData)
			for place, val := range cohort.Val {
				// When date is in the request, response date is the given date.
				// Otherwise, response date is the latest date from the cache.
				respDate := date
				if respDate == "" {
					respDate = cohort.PlaceToLatestDate[place]
				}
				// Check if there is observation from previous loops (higher ranked
				// cohort).
				pointStat, exist := result.Data[statVar].Stat[place]
				shouldSetValue := !exist
				// When observation exists from higher ranked cohort, but the current
				// cohort has later date and is not inferior facet (like wikidata),
				// prefer the current cohort.
				shouldResetValue := exist && respDate > pointStat.Date && !stat.IsInferiorFacetPb(cohort)
				if shouldSetValue || shouldResetValue {
					result.Data[statVar].Stat[place] = &pb.PointStat{
						Date:     respDate,
						Value:    proto.Float64(val),
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
		childPlacesMap, err := placein.GetPlacesIn(ctx, store, []string{parentPlace}, childType)
		if err != nil {
			return nil, err
		}
		childPlaces = childPlacesMap[parentPlace]
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
					metaHash := util.GetMetadataHash(metaData)
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
		Metadata: make(map[string]*pb.StatMetadata),
	}
	for _, statVar := range statVars {
		result.Data[statVar] = &pb.PlacePointStatAll{
			StatList: []*pb.PlacePointStat{},
		}
	}

	// Read from cache directly
	cacheData, err := stat.ReadStatCollection(
		ctx, store.BtGroup, bigtable.BtObsCollection,
		parentPlace, childType, statVars, dateKey,
	)
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
			metaData := stat.GetMetadata(cohort)
			metaHash := util.GetMetadataHash(metaData)
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
					Value: proto.Float64(val),
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
		childPlacesMap, err := placein.GetPlacesIn(ctx, store, []string{parentPlace}, childType)
		if err != nil {
			return nil, err
		}
		childPlaces = childPlacesMap[parentPlace]
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
				var metaHash string
				if pointValue != nil {
					if i == 0 {
						metaHash = util.GetMetadataHash(metaData)
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
