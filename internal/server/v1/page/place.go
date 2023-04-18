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

package page

import (
	"context"
	"hash/fnv"
	"math/rand"
	"regexp"
	"sort"
	"strings"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/convert"
	"github.com/datacommonsorg/mixer/internal/server/place"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/server/v0/propertyvalue"
	"github.com/datacommonsorg/mixer/internal/server/v1/observations"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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
	"Continent": {},
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

// PlacePage implements API for Mixer.PlacePage.
func PlacePage(
	ctx context.Context,
	in *pbv1.PlacePageRequest,
	store *store.Store,
) (*pbv1.PlacePageResponse, error) {
	node := in.GetNode()
	if err := util.CheckValidDCIDs([]string{node}); err != nil {
		return nil, err
	}
	category := in.GetCategory()
	if category == "" {
		return nil, status.Error(codes.InvalidArgument, "category should not be empty")
	}
	seed := in.GetSeed()
	newStatVars := in.GetNewStatVars()
	return getPlacePageDataHelper(ctx, node, newStatVars, seed, store, category)
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
func getPlaceType(ctx context.Context, store *store.Store, dcid string) (string, error) {
	resp, err := propertyvalue.GetPropertyValuesHelper(
		ctx, store, []string{dcid}, "typeOf", true)
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
func getLatestPop(ctx context.Context, store *store.Store, placeDcids []string) (
	map[string]int32, error) {
	if len(placeDcids) == 0 {
		return nil, nil
	}
	req := &pbv1.BulkObservationsSeriesRequest{
		Entities:  placeDcids,
		Variables: []string{"Count_Person"},
	}
	resp, err := observations.BulkSeries(ctx, req, store)
	if err != nil {
		return nil, err
	}
	result := map[string]int32{}
	for _, obsByVariable := range resp.ObservationsByVariable {
		// There should be only one item for "Count_Person"
		for _, obsByEntity := range obsByVariable.ObservationsByEntity {
			place := obsByEntity.Entity
			seriesList := obsByEntity.SeriesByFacet
			if len(seriesList) > 0 {
				topSeries := seriesList[0]
				latestValue := topSeries.Series[len(topSeries.Series)-1].Value
				result[place] = int32(*latestValue)
			}
		}
	}
	return result, nil
}

func getDcids(places []*pbv1.Place) []string {
	result := []string{}
	for _, dcid := range places {
		result = append(result, dcid.Dcid)
	}
	return result
}

// Fetch place page cache data for a list of places.
func fetchBtData(
	ctx context.Context,
	store *store.Store,
	places []string,
	statVars []string,
	category string,
) (
	map[string]*pb.StatVarSeries,
	map[string]*pb.PointStat,
	map[string]*pbv1.ObsCategories,
	error,
) {
	// Fetch place page cache data in parallel.
	action := [][]string{places, {category}}
	prefix := bigtable.BtPlacePageCategoricalPrefix
	btDataList, err := bigtable.Read(
		ctx,
		store.BtGroup,
		prefix,
		action,
		func(jsonRaw []byte) (interface{}, error) {
			var placePageData pbv1.LandingPageCache
			if err := proto.Unmarshal(jsonRaw, &placePageData); err != nil {
				return nil, err
			}
			return &placePageData, nil
		},
	)
	if err != nil {
		return nil, nil, nil, err
	}

	// Fetch categories data
	btCategoryData, err := bigtable.Read(
		ctx,
		store.BtGroup,
		bigtable.BtLandingPageCategories,
		[][]string{places},
		func(jsonRaw []byte) (interface{}, error) {
			var categories pbv1.ObsCategories
			if err := proto.Unmarshal(jsonRaw, &categories); err != nil {
				return nil, err
			}
			return &categories, nil
		},
	)
	if err != nil {
		return nil, nil, nil, err
	}
	categoryData := map[string]*pbv1.ObsCategories{}
	for _, place := range places {
		categoryData[place] = &pbv1.ObsCategories{Category: []string{}}
	}
	for _, btData := range btCategoryData {
		for _, row := range btData {
			if row.Data == nil {
				continue
			}
			place := row.Parts[0]
			categories := row.Data.(*pbv1.ObsCategories)
			categoryData[place].Category = util.MergeDedupe(
				categories.Category, categoryData[place].Category)
		}
	}

	// Populate result from place page cache
	pageData := map[string]*pb.StatVarSeries{}
	popData := map[string]*pb.PointStat{}
	mergedPlacePageData := map[string]*pbv1.LandingPageCache{}
	for _, btData := range btDataList {
		for _, row := range btData {
			if row.Data == nil {
				continue
			}
			place := row.Parts[0]
			placePageData := row.Data.(*pbv1.LandingPageCache)
			if _, ok := mergedPlacePageData[place]; !ok {
				mergedPlacePageData[place] = placePageData
			}
			for statVar, obsTimeSeries := range placePageData.Data {
				if _, ok := mergedPlacePageData[place].Data[statVar]; !ok {
					mergedPlacePageData[place].Data[statVar] = obsTimeSeries
				} else {
					mergedPlacePageData[place].Data[statVar].SourceSeries = stat.CollectDistinctSourceSeries(
						mergedPlacePageData[place].Data[statVar].SourceSeries,
						obsTimeSeries.SourceSeries,
					)
				}
			}
		}
	}

	for place, data := range mergedPlacePageData {
		finalData := &pb.StatVarSeries{Data: map[string]*pb.Series{}}
		for statVar, obsTimeSeries := range data.Data {
			series, _ := stat.GetBestSeries(obsTimeSeries, "", false /* useLatest */)
			finalData.Data[statVar] = series
			if statVar == "Count_Person" {
				popSeries, latestDate := stat.GetBestSeries(obsTimeSeries, "", true /* useLatest */)
				if popSeries != nil {
					if conversion, ok := convert.UnitMapping[popSeries.Metadata.Unit]; ok {
						popSeries.Metadata.Unit = conversion.Unit
						for date := range popSeries.Val {
							popSeries.Val[date] *= conversion.Scaling
						}
					}
					popData[place] = &pb.PointStat{
						Date:     *latestDate,
						Value:    proto.Float64(popSeries.Val[*latestDate]),
						Metadata: popSeries.Metadata,
					}
				}
			}
		}
		pageData[place] = finalData
	}

	// Fetch additional stats as requested.
	if len(statVars) > 0 {
		resp, err := observations.BulkSeries(ctx, &pbv1.BulkObservationsSeriesRequest{
			Entities:  places,
			Variables: statVars,
		}, store)
		if err != nil {
			return nil, popData, nil, err
		}
		// Add additional data to the cache result
		for _, obsByVariable := range resp.ObservationsByVariable {
			variable := obsByVariable.Variable
			for _, obsByEntity := range obsByVariable.ObservationsByEntity {
				place := obsByEntity.Entity
				if pageData[place] == nil {
					pageData[place] = &pb.StatVarSeries{Data: map[string]*pb.Series{}}
				}
				if len(obsByEntity.SeriesByFacet) == 0 {
					continue
				}
				pageData[place].Data[variable] = &pb.Series{
					Val:      map[string]float64{},
					Metadata: resp.Facets[obsByEntity.SeriesByFacet[0].Facet],
				}
				for _, point := range obsByEntity.SeriesByFacet[0].Series {
					pageData[place].Data[variable].Val[point.Date] = *point.Value
				}
			}
		}
	}
	// Delete the empty entries. This will be moved to cache generation.
	for _, statVarSeries := range pageData {
		for statVar, series := range statVarSeries.Data {
			if series == nil {
				delete(statVarSeries.Data, statVar)
			}
		}
	}
	return pageData, popData, categoryData, nil
}

// Pick child places with the largest average population.
// Returns a tuple of child place type, and list of child places.
func filterChildPlaces(childPlaces map[string][]*pbv1.Place) (string, []*pbv1.Place) {
	var maxCount int
	var resultPlaces []*pbv1.Place
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
func getPlacePageChildPlaces(
	ctx context.Context, store *store.Store, placedDcid, placeType string,
) (
	map[string][]*pbv1.Place, error,
) {
	children := []*pb.EntityInfo{}
	// ContainedIn places
	containedInPlaces, err := propertyvalue.GetPropertyValuesHelper(
		ctx, store, []string{placedDcid}, "containedInPlace", false)
	if err != nil {
		return nil, err
	}
	children = append(children, containedInPlaces[placedDcid]...)
	// GeoOverlaps places
	overlapPlaces, err := propertyvalue.GetPropertyValuesHelper(
		ctx, store, []string{placedDcid}, "geoOverlaps", false)
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
	result := map[string][]*pbv1.Place{}
	for _, child := range children {
		childTypes := trimTypes(child.Types)
		for _, childType := range childTypes {
			if _, ok := wantedTypes[childType]; ok {
				result[childType] = append(result[childType], &pbv1.Place{
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
	placePop, err := getLatestPop(ctx, store, placeDcids)
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

func getParentPlaces(ctx context.Context, store *store.Store, dcid string) (
	[]string, error) {
	placeToMetadata, err := place.GetPlaceMetadataHelper(ctx, []string{dcid}, store)
	if err != nil {
		return nil, err
	}
	result := []string{}
	if data, ok := placeToMetadata[dcid]; ok {
		for _, parent := range data.Parents {
			// Only want to include parents with type that is included in
			// allWantedPlaceTypes except and not type CensusZipCodeTabulationArea
			if _, ok := allWantedPlaceTypes[parent.Type]; ok {
				if parent.Type != "CensusZipCodeTabulationArea" {
					result = append(result, parent.Dcid)
				}
			}
		}
	}
	return result, nil
}

// Get similar places.
func getSimilarPlaces(
	ctx context.Context, store *store.Store, placeDcid, placeType string, seed int64,
) ([]string, error) {
	cohort, err := getCohort(placeType, placeDcid)
	if err != nil {
		return nil, err
	}
	if cohort == "" {
		return []string{}, nil
	}
	resp, err := propertyvalue.GetPropertyValuesHelper(
		ctx, store, []string{cohort}, "member", true)
	if err != nil {
		return nil, err
	}
	places := []*pbv1.Place{}
	for _, node := range resp[cohort] {
		if node.Dcid != placeDcid {
			places = append(places, &pbv1.Place{
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
	rand.New(rand.NewSource(seed))
	rand.Shuffle(len(places), func(i, j int) {
		places[i], places[j] = places[j], places[i]
	})
	result := []*pbv1.Place{}
	for _, place := range places {
		result = append(result, place)
		if len(result) == maxSimilarPlace {
			return getDcids(result), nil
		}
	}
	return getDcids(result), nil

}

// Get nearby places.
func getNearbyPlaces(ctx context.Context, store *store.Store, dcid string,
) ([]string, error) {
	resp, err := propertyvalue.GetPropertyValuesHelper(
		ctx, store, []string{dcid}, "nearbyPlaces", true)
	if err != nil {
		return nil, err
	}
	places := []string{}
	for _, node := range resp[dcid] {
		tokens := strings.Split(node.Value, "@")
		places = append(places, tokens[0])
	}
	placePop, err := getLatestPop(ctx, store, places)
	if err != nil {
		return nil, err
	}
	result := []*pbv1.Place{}
	for dcid, pop := range placePop {
		if pop > minPopulation {
			result = append(result, &pbv1.Place{
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

// getPlacePageDataHelper is a wrapper to get place page data.
//
// TODO(shifucun):For each related place, it is supposed to have dcid, name and
// population but it's not complete now as the client in most cases only requires
// the dcid. Should consider have the full name, even with parent place
// abbreviations like "CA" filled in here so the client won't bother to fetch
// those again.
func getPlacePageDataHelper(
	ctx context.Context,
	placeDcid string,
	newStatVars []string,
	seed int64,
	store *store.Store,
	category string,
) (*pbv1.PlacePageResponse, error) {
	placeType, err := getPlaceType(ctx, store, placeDcid)
	if err != nil {
		return nil, err
	}

	// Fetch child and parent places in go routines.
	errs, errCtx := errgroup.WithContext(ctx)
	relatedPlaceChan := make(chan *relatedPlace, 4)
	allChildPlaceChan := make(chan map[string][]*pbv1.Place, 1)
	var filteredChildPlaceType string
	errs.Go(func() error {
		childPlaces, err := getPlacePageChildPlaces(errCtx, store, placeDcid, placeType)
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
		parentPlaces, err := getParentPlaces(errCtx, store, placeDcid)
		if err != nil {
			return err
		}
		relatedPlaceChan <- &relatedPlace{category: parentEnum, places: parentPlaces}
		return nil
	})
	errs.Go(func() error {
		similarPlaces, err := getSimilarPlaces(errCtx, store, placeDcid, placeType, seed)
		if err != nil {
			return err
		}
		relatedPlaceChan <- &relatedPlace{category: similarEnum, places: similarPlaces}
		return nil
	})
	errs.Go(func() error {
		nearbyPlaces, err := getNearbyPlaces(errCtx, store, placeDcid)
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

	resp := pbv1.PlacePageResponse{}

	allChildPlaces := map[string]*pbv1.Places{}
	for tmp := range allChildPlaceChan {
		for k, places := range tmp {
			allChildPlaces[k] = &pbv1.Places{Places: places}
		}
	}
	resp.AllChildPlaces = allChildPlaces
	resp.ChildPlacesType = filteredChildPlaceType

	// Fetch the place page stats data for all places.
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

	statData, popData, categoryData, err := fetchBtData(
		ctx, store, allPlaces, newStatVars, category)
	if err != nil {
		return nil, err
	}
	resp.StatVarSeries = statData
	resp.LatestPopulation = popData
	resp.ValidCategories = categoryData
	return &resp, nil
}
