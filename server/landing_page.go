// Copyright 2019 Google LLC
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

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/util"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxNumChild     = 5
	maxSimilarPlace = 5
	maxNearbyPlace  = 5
	minPopulation   = 10000
	cityCohort      = "PlacePagesComparisonCityCohort"
	countyCohort    = "PlacePagesComparisonCountyCohort"
)

const (
	childEnum   = "child"
	parentEnum  = "parent"
	similarEnum = "similar"
	nearbyEnum  = "nearby"
)

type relatedPlace struct {
	category string
	places   []string
}

var wantedPlaceTypes = map[string]map[string]struct{}{
	"Country": {
		"State":               {},
		"EurostatNUTS1":       {},
		"EurostatNUTS2":       {},
		"AdministrativeArea1": {},
	},
	"State": {
		"County": {},
	},
	"County": {
		"City":    {},
		"Town":    {},
		"Village": {},
		"Borough": {},
	},
}

var allWantedPlaceTypes = map[string]struct{}{
	"Country": {}, "State": {}, "County": {}, "City": {}, "Town": {}, "Village": {}, "Borough": {},
	"CensusZipCodeTabulationArea": {}, "EurostatNUTS1": {}, "EurostatNUTS2": {},
	"EurostatNUTS3": {}, "AdministrativeArea1": {}, "AdministrativeArea2": {},
	"AdministrativeArea3": {}, "AdministrativeArea4": {}, "AdministrativeArea5": {},
}

// These place types are equivalent: prefer the key.
var equivalentPlaceTypes = map[string]string{
	"State":   "AdministrativeArea1",
	"County":  "AdministrativeArea2",
	"City":    "AdministrativeArea3",
	"Town":    "City",
	"Borough": "City",
	"Village": "City",
}

var continents = map[string]struct{}{
	"Africa":        {},
	"Antarctica":    {},
	"Asia":          {},
	"Europe":        {},
	"North America": {},
	"Oceania":       {},
	"South America": {},
}

// A lot of the code below mimics the logic from website server:
// https://github.com/datacommonsorg/website/blob/45ede51440f85597920abeb2f7b7531ccd50e9dc/server/routes/api/place.py

// get the type of a place.
func getPlaceType(ctx context.Context, s *Server, dcid string) (string, error) {
	resp, err := getPropertyValuesHelper(
		ctx, s.btTable, s.memcache, []string{dcid}, "typeOf", true)
	if err != nil {
		return "", err
	}
	types := []string{}
	for _, node := range resp[dcid] {
		types = append(types, node.Dcid)
	}
	chosenType := ""
	for _, placeType := range types {
		if chosenType == "" ||
			strings.HasPrefix(chosenType, "AdministrativeArea") ||
			chosenType == "Place" {
			chosenType = placeType
		}
	}
	return chosenType, nil
}

// When there are equivalent types, only choose the primary type.
func trimTypes(types []string) []string {
	result := []string{}
	toTrim := map[string]struct{}{}
	for _, typ := range types {
		if other, ok := equivalentPlaceTypes[typ]; ok {
			toTrim[other] = struct{}{}
		}
	}
	for _, typ := range types {
		if _, ok := toTrim[typ]; !ok {
			result = append(result, typ)
		}
	}
	return result
}

// Get the latest population count for a list of places.
func getLatestPop(ctx context.Context, s *Server, placeDcids []string) (
	map[string]int32, error) {
	if len(placeDcids) == 0 {
		return nil, nil
	}
	req := &pb.GetStatsRequest{
		Place:    placeDcids,
		StatsVar: "Count_Person",
	}
	resp, err := s.GetStats(ctx, req)
	if err != nil {
		return nil, err
	}
	result := map[string]int32{}
	tmp := map[string]*ObsTimeSeries{}
	err = json.Unmarshal([]byte(resp.Payload), &tmp)
	if err != nil {
		return nil, err
	}
	for place, series := range tmp {
		if series != nil {
			latestDate := ""
			latestValue := 0.0
			for date, value := range series.Data {
				if date > latestDate {
					latestValue = value
					latestDate = date
				}
			}
			if latestDate != "" {
				result[place] = int32(latestValue)
			}
		}
	}
	return result, nil
}

func getDcids(places []*place) []string {
	result := []string{}
	for _, dcid := range places {
		result = append(result, dcid.Dcid)
	}
	return result
}

// Fetch landing page cache data for a list of places.
func fetchBtData(
	ctx context.Context, s *Server, places []string, statVars []string) (
	map[string]map[string]*ObsTimeSeries, error) {
	rowList := bigtable.RowList{}
	for _, dcid := range places {
		rowList = append(rowList, fmt.Sprintf(
			"%s%s", util.BtLandingPagePrefix, dcid))
	}

	// Fetch landing page cache data in parallel.
	dataMap, err := bigTableReadRowsParallel(ctx, s.btTable, rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			var landingPageData LandingPageData

			err := json.Unmarshal(jsonRaw, &landingPageData)
			if err != nil {
				return nil, err
			}
			return &landingPageData, nil
		}, nil)
	if err != nil {
		return nil, err
	}
	// Populate result from landing page cache
	result := map[string]map[string]*ObsTimeSeries{}
	for dcid, data := range dataMap {
		landingPageData := data.(*LandingPageData)
		finalData := map[string]*ObsTimeSeries{}
		for statVarDcid, obsTimeSeries := range landingPageData.Data {
			obsTimeSeries.filterAndRank(&obsProp{})
			finalData[statVarDcid] = obsTimeSeries
		}
		result[dcid] = finalData
	}

	// The landing page cache may depends on out dated chart config. Fetch stats
	// that are not included in the landing page cache.
	gotStatVars := map[string]struct{}{}
	for statVar := range result[places[0]] {
		gotStatVars[statVar] = struct{}{}
	}
	missingStatVars := []string{}
	for _, statVar := range statVars {
		if _, ok := gotStatVars[statVar]; !ok {
			missingStatVars = append(missingStatVars, statVar)
		}
	}
	if len(missingStatVars) > 0 {
		type svData struct {
			sv   string
			data map[string]*ObsTimeSeries
		}
		errs, errCtx := errgroup.WithContext(ctx)
		dataChan := make(chan svData, len(missingStatVars))
		for _, statVar := range missingStatVars {
			statVar := statVar
			errs.Go(func() error {
				resp, err := s.GetStats(errCtx, &pb.GetStatsRequest{
					Place:    places,
					StatsVar: statVar,
				})
				if err != nil {
					return err
				}
				var tmp map[string]*ObsTimeSeries
				err = json.Unmarshal([]byte(resp.Payload), &tmp)
				if err != nil {
					return err
				}
				dataChan <- svData{sv: statVar, data: tmp}
				return nil
			})
		}
		err := errs.Wait()
		if err != nil {
			return nil, err
		}
		close(dataChan)
		for item := range dataChan {
			statVar := item.sv
			for place := range item.data {
				if result[place] != nil {
					result[place][statVar] = item.data[place]
					if result[place][statVar] == nil {
						result[place][statVar] = &ObsTimeSeries{}
					}
					result[place][statVar].PlaceName = ""
				}
			}
		}
	}
	return result, nil
}

// Pick child places with the largest average population.
// Returns a tuple of child place type, and list of child places.
func filterChildPlaces(childPlaces map[string][]*place) (string, []*place) {
	var maxCount int
	var resultPlaces []*place
	var resultType string
	for childType, children := range childPlaces {
		if len(children) > maxCount {
			maxCount = len(children)
			resultPlaces = children
			resultType = childType
		}
	}
	if len(resultPlaces) > maxNumChild {
		resultPlaces = resultPlaces[0:maxNumChild]
	}
	return resultType, resultPlaces
}

// Get child places by types.
// The place under each type is sorted by the population.
func getChildPlaces(ctx context.Context, s *Server, dcid string) (
	map[string][]*place, error) {
	children := []*Node{}
	// ContainedIn places
	containedInPlaces, err := getPropertyValuesHelper(
		ctx, s.btTable, s.memcache, []string{dcid}, "containedInPlace", false)
	if err != nil {
		return nil, err
	}
	children = append(children, containedInPlaces[dcid]...)
	// GeoOverlaps places
	overlapPlaces, err := getPropertyValuesHelper(
		ctx, s.btTable, s.memcache, []string{dcid}, "geoOverlaps", false)
	if err != nil {
		return nil, err
	}
	children = append(children, overlapPlaces[dcid]...)
	// Get the wanted place types
	placeType, err := getPlaceType(ctx, s, dcid)
	if err != nil {
		return nil, err
	}
	wantedTypes, ok := wantedPlaceTypes[placeType]
	if !ok {
		wantedTypes = allWantedPlaceTypes
	}
	// Populate result
	result := map[string][]*place{}
	for _, child := range children {
		childTypes := trimTypes(child.Types)
		for _, childType := range childTypes {
			if _, ok := wantedTypes[childType]; ok {
				result[childType] = append(result[childType], &place{
					Dcid: child.Dcid,
					Name: child.Name,
				})
			}
		}
	}
	// Get the population for child places
	placeDcids := []string{}
	for _, children := range result {
		for _, child := range children {
			placeDcids = append(placeDcids, child.Dcid)
		}
	}
	placePop, err := getLatestPop(ctx, s, placeDcids)
	if err != nil {
		return nil, err
	}
	for _, children := range result {
		for _, child := range children {
			if val, ok := placePop[child.Dcid]; ok {
				child.Pop = val
			}
		}
	}
	// Drop empty categories and sort the children by population
	for typ := range result {
		if len(result[typ]) == 0 {
			delete(result, typ)
		} else {
			sort.SliceStable(result[typ], func(i, j int) bool {
				return result[typ][i].Pop > result[typ][j].Pop
			})
		}
	}

	return result, nil
}

// Get parent places up to continent level.
func getParentPlaces(ctx context.Context, s *Server, dcid string) (
	[]string, error) {
	result := []*place{}
	for {
		containedInPlaces, err := getPropertyValuesHelper(
			ctx, s.btTable, s.memcache, []string{dcid}, "containedInPlace", true)
		if err != nil {
			return nil, err
		}
		sort.SliceStable(containedInPlaces[dcid], func(i, j int) bool {
			return containedInPlaces[dcid][i].Dcid > containedInPlaces[dcid][j].Dcid
		})
		for _, parent := range containedInPlaces[dcid] {
			if parent.Types[0] == "CensusZipCodeTabulationArea" {
				continue
			}
			result = append(result, &place{
				Dcid: parent.Dcid,
				Name: parent.Name,
			})
		}
		if len(result) == 0 {
			break
		}
		if _, ok := continents[result[len(result)-1].Name]; ok {
			break
		}
		dcid = result[len(result)-1].Dcid
	}
	return getDcids(result), nil
}

// Get similar places.
func getSimilarPlaces(ctx context.Context, s *Server, dcid string, seed int64) (
	[]string, error) {

	isCounty, err := regexp.MatchString(`^geoId/\d{5}$`, dcid)
	if err != nil {
		return nil, err
	}
	isCity, err := regexp.MatchString(`^geoId/\d{7}$`, dcid)
	if err != nil {
		return nil, err
	}
	if isCity || isCounty {
		geoID, err := strconv.Atoi(strings.TrimPrefix(dcid, "geoId/"))
		if err != nil {
			return nil, err
		}
		// Seed with day of the year and place dcid to make it relatively stable
		// in a day.
		if seed == 0 {
			seed = int64(time.Now().YearDay() + geoID)
		}
		rand.Seed(seed)
		var cohort string
		if isCity {
			cohort = cityCohort
		} else {
			cohort = countyCohort
		}
		resp, err := getPropertyValuesHelper(
			ctx, s.btTable, s.memcache, []string{cohort}, "member", true)
		if err != nil {
			return nil, err
		}
		places := []*place{}
		for _, node := range resp[cohort] {
			if node.Dcid != dcid {
				places = append(places, &place{
					Dcid: node.Dcid,
					Name: node.Name,
				})
			}
		}
		// Shuffle places to get random results at different query time.
		rand.Shuffle(len(places), func(i, j int) {
			places[i], places[j] = places[j], places[i]
		})
		result := []*place{}
		for _, place := range places {
			result = append(result, place)
			if len(result) == maxSimilarPlace {
				return getDcids(result), nil
			}
		}
		return getDcids(result), nil
	}
	// For non US city and county, use related places.
	parents, err := getParentPlaces(ctx, s, dcid)
	if err != nil {
		return nil, err
	}
	parentDcid := ""
	if len(parents) >= 2 {
		parentDcid = parents[len(parents)-2]
	}
	resp, err := s.GetRelatedLocations(ctx, &pb.GetRelatedLocationsRequest{
		Dcid:         dcid,
		StatVarDcids: []string{"Count_Person"},
		WithinPlace:  parentDcid,
	})
	if err != nil {
		return nil, err
	}
	var relatedPlaceData map[string]*RelatedPlacesInfo
	err = json.Unmarshal([]byte(resp.Payload), &relatedPlaceData)
	if err != nil {
		return nil, err
	}
	if relatedPlaceData["Count_Person"] == nil {
		return []string{}, nil
	}
	return relatedPlaceData["Count_Person"].RelatedPlaces, nil
}

// Get nearby places.
func getNearbyPlaces(ctx context.Context, s *Server, dcid string) (
	[]string, error) {

	resp, err := getPropertyValuesHelper(
		ctx, s.btTable, s.memcache, []string{dcid}, "nearbyPlaces", true)
	if err != nil {
		return nil, err
	}
	places := []string{}
	for _, node := range resp[dcid] {
		tokens := strings.Split(node.Value, "@")
		places = append(places, tokens[0])
	}
	placePop, err := getLatestPop(ctx, s, places)
	if err != nil {
		return nil, err
	}
	result := []*place{}
	for dcid, pop := range placePop {
		if pop > minPopulation {
			result = append(result, &place{
				Dcid: dcid,
				Pop:  pop,
			})
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].Pop > result[j].Pop
	})
	if len(result) < maxNearbyPlace {
		return getDcids(result), nil
	}
	return getDcids(result[0:maxNearbyPlace]), nil
}

// GetLandingPageData implements API for Mixer.GetLandingPageData.
//
// TODO(shifucun):For each related place, it is supposed to have dcid, name and
// population but it's not complete now as the client in most cases only requires
// the dcid. Should consider have the full name, even with parent place
// abbreviations like "CA" filled in here so the client won't bother to fetch
// those again.
func (s *Server) GetLandingPageData(
	ctx context.Context, in *pb.GetLandingPageDataRequest) (
	*pb.GetLandingPageDataResponse, error) {
	placeDcid := in.GetPlace()
	if placeDcid == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Missing required arguments: dcid")
	}
	seed := in.GetSeed()
	statVars := in.GetStatVars()

	// Fetch child and prarent places in go routines.
	errs, errCtx := errgroup.WithContext(ctx)
	relatedPlaceChan := make(chan *relatedPlace, 4)
	allChildPlaceChan := make(chan map[string][]*place, 1)
	var filteredChildPlaceType string
	errs.Go(func() error {
		childPlaces, err := getChildPlaces(errCtx, s, placeDcid)
		if err != nil {
			return err
		}
		allChildPlaceChan <- childPlaces
		childPlaceType, childPlaceList := filterChildPlaces(childPlaces)
		filteredChildPlaceType = childPlaceType
		relatedPlaceChan <- &relatedPlace{category: childEnum, places: getDcids(childPlaceList)}
		return nil
	})
	errs.Go(func() error {
		parentPlaces, err := getParentPlaces(errCtx, s, placeDcid)
		if err != nil {
			return err
		}
		relatedPlaceChan <- &relatedPlace{category: parentEnum, places: parentPlaces}
		return nil
	})
	errs.Go(func() error {
		similarPlaces, err := getSimilarPlaces(errCtx, s, placeDcid, seed)
		if err != nil {
			return err
		}
		relatedPlaceChan <- &relatedPlace{category: similarEnum, places: similarPlaces}
		return nil
	})
	errs.Go(func() error {
		nearbyPlaces, err := getNearbyPlaces(errCtx, s, placeDcid)
		if err != nil {
			return err
		}
		relatedPlaceChan <- &relatedPlace{category: nearbyEnum, places: nearbyPlaces}
		return nil
	})

	err := errs.Wait()
	if err != nil {
		return nil, err
	}
	close(allChildPlaceChan)
	close(relatedPlaceChan)

	payload := LandingPageResponse{}

	var allChildPlaces map[string][]*place
	for tmp := range allChildPlaceChan {
		allChildPlaces = tmp
		break
	}
	payload.AllChildPlaces = allChildPlaces
	payload.ChildPlacesType = filteredChildPlaceType

	// Fetch the landing page stats data for all places.
	allPlaces := []string{placeDcid}
	for relatedPlace := range relatedPlaceChan {
		switch relatedPlace.category {
		case childEnum:
			payload.ChildPlaces = relatedPlace.places
		case parentEnum:
			payload.ParentPlaces = relatedPlace.places
		case similarEnum:
			payload.SimilarPlaces = relatedPlace.places
		case nearbyEnum:
			payload.NearbyPlaces = relatedPlace.places
		default:
		}
		allPlaces = append(allPlaces, relatedPlace.places...)
	}
	statData, err := fetchBtData(ctx, s, allPlaces, statVars)
	if err != nil {
		return nil, err
	}
	payload.Data = statData
	jsonRaw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &pb.GetLandingPageDataResponse{Payload: string(jsonRaw)}, nil
}
