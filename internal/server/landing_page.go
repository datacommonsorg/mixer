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
	"hash/fnv"
	"math/rand"
	"regexp"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	maxNumChild     = 5
	maxSimilarPlace = 5
	maxNearbyPlace  = 5
	minPopulation   = 10000
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

func getCohort(placeType string, placeDcid string) (string, error) {
	// Country
	if placeType == "Country" {
		return "PlacePagesComparisonCountriesCohort", nil
	}
	// US State
	ok, err := regexp.MatchString(`^geoId/\d{2}$`, placeDcid)
	if err != nil {
		return "", err
	}
	if ok {
		return "PlacePagesComparisonStateCohort", nil
	}
	// US County
	ok, err = regexp.MatchString(`^geoId/\d{5}$`, placeDcid)
	if err != nil {
		return "", err
	}
	if ok {
		return "PlacePagesComparisonCountyCohort", nil
	}
	// US City
	ok, err = regexp.MatchString(`^geoId/\d{7}$`, placeDcid)
	if err != nil {
		return "", err
	}
	if ok {
		return "PlacePagesComparisonCityCohort", nil
	}
	// World cities
	if placeType == "City" {
		return "PlacePagesComparisonWorldCitiesCohort", nil
	}
	return "", nil
}

// A lot of the code below mimics the logic from website server:
// https://github.com/datacommonsorg/website/blob/45ede51440f85597920abeb2f7b7531ccd50e9dc/server/routes/api/place.py

// get the type of a place.
func getPlaceType(ctx context.Context, s *Server, dcid string) (string, error) {
	resp, err := getPropertyValuesHelper(
		ctx, s.store, []string{dcid}, "typeOf", true)
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

func getDcids(places []*pb.Place) []string {
	result := []string{}
	for _, dcid := range places {
		result = append(result, dcid.Dcid)
	}
	return result
}

// Fetch landing page cache data for a list of places.
func fetchBtData(
	ctx context.Context, s *Server, places []string, statVars []string) (
	map[string]*pb.StatVarSeries, error) {
	rowList := bigtable.RowList{}
	for _, dcid := range places {
		rowList = append(rowList, fmt.Sprintf(
			"%s%s", util.BtLandingPagePrefix, dcid))
	}

	// Fetch landing page cache data in parallel.
	// Landing page cache only exists in base cache
	baseDataMap, _, err := bigTableReadRowsParallel(
		ctx,
		s.store,
		rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			var landingPageData pb.StatVarObsSeries
			err := protojson.Unmarshal(jsonRaw, &landingPageData)
			if err != nil {
				return nil, err
			}
			return &landingPageData, nil
		},
		nil,
		false, /* readBranch */
	)
	if err != nil {
		return nil, err
	}
	// Populate result from landing page cache
	result := map[string]*pb.StatVarSeries{}
	for dcid, data := range baseDataMap {
		if data == nil {
			continue
		}
		landingPageData := data.(*pb.StatVarObsSeries)
		finalData := &pb.StatVarSeries{Data: map[string]*pb.Series{}}
		for statVarDcid, obsTimeSeries := range landingPageData.Data {
			finalData.Data[statVarDcid] = getBestSeries(obsTimeSeries)
		}
		result[dcid] = finalData
	}

	// Fetch additional stats as requested.
	if len(statVars) > 0 {
		resp, err := s.GetStatSetSeries(ctx, &pb.GetStatSetSeriesRequest{
			Places:   places,
			StatVars: statVars,
		})
		if err != nil {
			return nil, err
		}
		// Add additional data to the cache result
		for place, seriesMap := range resp.Data {
			for statVar, series := range seriesMap.Data {
				if result[place] == nil {
					result[place] = &pb.StatVarSeries{Data: map[string]*pb.Series{}}
				}
				result[place].Data[statVar] = series
			}
		}
	}
	// Delete the empty entries. This will be moved to cache generation.
	for _, statVarSeries := range result {
		for statVar, series := range statVarSeries.Data {
			if series == nil {
				delete(statVarSeries.Data, statVar)
			}
		}
	}
	return result, nil
}

// Pick child places with the largest average population.
// Returns a tuple of child place type, and list of child places.
func filterChildPlaces(childPlaces map[string][]*pb.Place) (string, []*pb.Place) {
	var maxCount int
	var resultPlaces []*pb.Place
	var resultType string

	// Sort child types to get stable result.
	childTypes := make([]string, 0, len(childPlaces))
	for k := range childPlaces {
		childTypes = append(childTypes, k)
	}
	sort.Strings(childTypes)

	for _, childType := range childTypes {
		children := childPlaces[childType]
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
func getChildPlaces(ctx context.Context, s *Server, placedDcid, placeType string) (
	map[string][]*pb.Place, error) {
	children := []*Node{}
	// ContainedIn places
	containedInPlaces, err := getPropertyValuesHelper(
		ctx, s.store, []string{placedDcid}, "containedInPlace", false)
	if err != nil {
		return nil, err
	}
	children = append(children, containedInPlaces[placedDcid]...)
	// GeoOverlaps places
	overlapPlaces, err := getPropertyValuesHelper(
		ctx, s.store, []string{placedDcid}, "geoOverlaps", false)
	if err != nil {
		return nil, err
	}
	children = append(children, overlapPlaces[placedDcid]...)
	// Get the wanted place types
	wantedTypes, ok := wantedPlaceTypes[placeType]
	if !ok {
		wantedTypes = allWantedPlaceTypes
	}
	// Populate result
	result := map[string][]*pb.Place{}
	for _, child := range children {
		childTypes := trimTypes(child.Types)
		for _, childType := range childTypes {
			if _, ok := wantedTypes[childType]; ok {
				result[childType] = append(result[childType], &pb.Place{
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
	result := []*pb.Place{}
	for {
		containedInPlaces, err := getPropertyValuesHelper(
			ctx, s.store, []string{dcid}, "containedInPlace", true)
		if err != nil {
			return nil, err
		}
		if len(containedInPlaces) == 0 {
			// There may be ancestors without any parents.
			break
		}
		sort.SliceStable(containedInPlaces[dcid], func(i, j int) bool {
			return containedInPlaces[dcid][i].Dcid > containedInPlaces[dcid][j].Dcid
		})
		for _, parent := range containedInPlaces[dcid] {
			if parent.Types[0] == "CensusZipCodeTabulationArea" {
				continue
			}
			result = append(result, &pb.Place{
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
func getSimilarPlaces(
	ctx context.Context, s *Server, placeDcid, placeType string, seed int64) (
	[]string, error) {

	cohort, err := getCohort(placeType, placeDcid)
	if err != nil {
		return nil, err
	}
	if cohort == "" {
		return []string{}, nil
	}
	resp, err := getPropertyValuesHelper(
		ctx, s.store, []string{cohort}, "member", true)
	if err != nil {
		return nil, err
	}
	places := []*pb.Place{}
	for _, node := range resp[cohort] {
		if node.Dcid != placeDcid {
			places = append(places, &pb.Place{
				Dcid: node.Dcid,
				Name: node.Name,
			})
		}
	}
	// Shuffle places to get random results at different query time.
	if seed == 0 {
		h := fnv.New32a()
		_, err = h.Write([]byte(placeDcid))
		if err != nil {
			return nil, err
		}
		seed = int64(time.Now().YearDay() + int(h.Sum32()))
	}
	rand.Seed(seed)
	rand.Shuffle(len(places), func(i, j int) {
		places[i], places[j] = places[j], places[i]
	})
	result := []*pb.Place{}
	for _, place := range places {
		result = append(result, place)
		if len(result) == maxSimilarPlace {
			return getDcids(result), nil
		}
	}
	return getDcids(result), nil

}

// Get nearby places.
func getNearbyPlaces(ctx context.Context, s *Server, dcid string) (
	[]string, error) {

	resp, err := getPropertyValuesHelper(
		ctx, s.store, []string{dcid}, "nearbyPlaces", true)
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
	result := []*pb.Place{}
	for dcid, pop := range placePop {
		if pop > minPopulation {
			result = append(result, &pb.Place{
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
	newStatVars := in.GetNewStatVars()

	placeType, err := getPlaceType(ctx, s, placeDcid)
	if err != nil {
		return nil, err
	}

	// Fetch child and prarent places in go routines.
	errs, errCtx := errgroup.WithContext(ctx)
	relatedPlaceChan := make(chan *relatedPlace, 4)
	allChildPlaceChan := make(chan map[string][]*pb.Place, 1)
	var filteredChildPlaceType string
	errs.Go(func() error {
		childPlaces, err := getChildPlaces(errCtx, s, placeDcid, placeType)
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
		similarPlaces, err := getSimilarPlaces(errCtx, s, placeDcid, placeType, seed)
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

	err = errs.Wait()
	if err != nil {
		return nil, err
	}
	close(allChildPlaceChan)
	close(relatedPlaceChan)

	resp := pb.GetLandingPageDataResponse{}

	allChildPlaces := map[string]*pb.Places{}
	for tmp := range allChildPlaceChan {
		for k, places := range tmp {
			allChildPlaces[k] = &pb.Places{Places: places}
		}
	}
	resp.AllChildPlaces = allChildPlaces
	resp.ChildPlacesType = filteredChildPlaceType

	// Fetch the landing page stats data for all places.
	allPlaces := []string{placeDcid}
	for relatedPlace := range relatedPlaceChan {
		switch relatedPlace.category {
		case childEnum:
			resp.ChildPlaces = relatedPlace.places
		case parentEnum:
			resp.ParentPlaces = relatedPlace.places
		case similarEnum:
			resp.SimilarPlaces = relatedPlace.places
		case nearbyEnum:
			resp.NearbyPlaces = relatedPlace.places
		default:
		}
		allPlaces = append(allPlaces, relatedPlace.places...)
	}
	statData, err := fetchBtData(ctx, s, allPlaces, newStatVars)
	if err != nil {
		return nil, err
	}
	resp.StatVarSeries = statData
	return &resp, nil
}
