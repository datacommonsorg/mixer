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
	"fmt"
	"strings"

	"encoding/json"

	"cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/util"
)

// GetPlacesInPost implements API for Mixer.GetPlacesInPost.
func (s *Server) GetPlacesInPost(ctx context.Context,
	in *pb.GetPlacesInRequest) (*pb.GetPlacesInResponse, error) {
	return s.GetPlacesIn(ctx, in)
}

// GetPlacesIn implements API for Mixer.GetPlacesIn.
func (s *Server) GetPlacesIn(ctx context.Context, in *pb.GetPlacesInRequest) (
	*pb.GetPlacesInResponse, error) {
	dcids := in.GetDcids()
	placeType := in.GetPlaceType()

	if len(dcids) == 0 || placeType == "" {
		return nil, fmt.Errorf("missing required arguments")
	}
	if !util.CheckValidDCIDs(dcids) {
		return nil, fmt.Errorf("invalid DCIDs")
	}

	rowList := buildPlaceInKey(dcids, placeType)

	dataMap, err := bigTableReadRowsParallel(ctx, s.btTable, rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			return strings.Split(string(jsonRaw), ","), nil
		})
	if err != nil {
		return nil, err
	}
	results := []map[string]string{}
	for dcid, data := range dataMap {

		for _, place := range data.([]string) {
			results = append(results, map[string]string{"dcid": dcid, "place": place})
		}
	}

	jsonRaw, err := json.Marshal(results)
	if err != nil {
		return nil, err
	}
	return &pb.GetPlacesInResponse{Payload: string(jsonRaw)}, nil
}

// GetRelatedPlaces implements API for Mixer.GetRelatedPlaces.
func (s *Server) GetRelatedPlaces(ctx context.Context,
	in *pb.GetRelatedPlacesRequest) (*pb.GetRelatedPlacesResponse, error) {
	if len(in.GetDcids()) == 0 || in.GetPopulationType() == "" ||
		in.GetMeasuredProperty() == "" || in.GetStatType() == "" {
		return nil, fmt.Errorf("missing required arguments")
	}
	if !util.CheckValidDCIDs(in.GetDcids()) {
		return nil, fmt.Errorf("invalid DCIDs")
	}

	// TODO: Move the default value up to Python API. Consult wsws before moving.
	//
	// The default values ensure for the following 5 cache keys.
	// count^CensusACS5yrSurvey^^^^^measuredValue^Person
	// income^CensusACS5yrSurvey^^^^USDollar^medianValue^Person^age^Years15Onwards^ \
	//   incomeStatus^WithIncome
	// age^CensusACS5yrSurvey^^^^Year^medianValue^Person
	// unemploymentRate^BLSSeasonallyUnadjusted^^^^^measuredValue^Person
	// count^^^^^^measuredValue^CriminalActivities^crimeType^UCR_CombinedCrime
	measuredProperty := in.GetMeasuredProperty()
	populationType := in.GetPopulationType()
	measurementMethod := in.GetMeasurementMethod()
	if measurementMethod == "" && populationType == "Person" {
		if measuredProperty == "unemploymentRate" {
			measurementMethod = "BLSSeasonallyUnadjusted"
		} else {
			measurementMethod = "CensusACS5yrSurvey"
		}
	}
	unit := in.GetUnit()
	if unit == "" {
		if measuredProperty == "age" {
			unit = "Year"
		} else if measuredProperty == "income" {
			unit = "USDollar"
		}
	}

	popObsSignatureItems := []string{
		measuredProperty,
		measurementMethod,
		in.GetMeasurementDenominator(),
		in.GetMeasurementQualifier(),
		in.GetScalingFactor(),
		unit,
		in.GetStatType(),
		in.GetPopulationType(),
	}
	if len(in.GetPvs()) > 0 {
		iterateSortPVs(in.GetPvs(), func(i int, p, v string) {
			popObsSignatureItems = append(popObsSignatureItems, []string{p, v}...)
		})
	}
	popObsSignature := strings.Join(popObsSignatureItems, "^")

	// TODO: Currently same_place_type and within_place options are mutually exclusive.
	// The logic here chooses within_place when both set.
	// Remove the logic when both options can live together.
	withinPlace := in.GetWithinPlace()
	if withinPlace == "country/USA" {
		withinPlace = ""
	}
	samePlaceType := in.GetSamePlaceType()
	isPerCapita := in.GetIsPerCapita()
	var prefix string
	if withinPlace == "" {
		if samePlaceType {
			prefix = util.BtRelatedPlacesSameTypePrefix
			if isPerCapita {
				prefix = util.BtRelatedPlacesSameTypePCPrefix
			}
		} else {
			prefix = util.BtRelatedPlacesPrefix
			if isPerCapita {
				prefix = util.BtRelatedPlacesPCPrefix
			}
		}
	} else {
		if samePlaceType {
			prefix = util.BtRelatedPlacesSameTypeAndAncestorPrefix
			if isPerCapita {
				prefix = util.BtRelatedPlacesSameTypeAndAncestorPCPrefix
			}
		} else {
			prefix = util.BtRelatedPlacesSameAncestorPrefix
			if isPerCapita {
				prefix = util.BtRelatedPlacesSameAncestorPCPrefix
			}
		}
	}

	dcids := in.GetDcids()
	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		if withinPlace != "" {
			rowList = append(rowList, fmt.Sprintf(
				"%s%s^%s^%s", prefix, dcid, withinPlace, popObsSignature))
		} else {
			rowList = append(rowList, fmt.Sprintf(
				"%s%s^%s", prefix, dcid, popObsSignature))
		}
	}
	dataMap, err := bigTableReadRowsParallel(ctx, s.btTable, rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			var btRelatedPlacesInfo RelatedPlacesInfo
			err := json.Unmarshal(jsonRaw, &btRelatedPlacesInfo)
			if err != nil {
				return nil, err
			}
			return &btRelatedPlacesInfo, nil
		})
	if err != nil {
		return nil, err
	}
	results := map[string]*RelatedPlacesInfo{}
	for dcid, data := range dataMap {
		results[dcid] = data.(*RelatedPlacesInfo)
	}
	jsonRaw, err := json.Marshal(results)
	if err != nil {
		return nil, err
	}
	return &pb.GetRelatedPlacesResponse{Payload: string(jsonRaw)}, nil
}

// GetInterestingPlaceAspects implements API for Mixer.GetInterestingPlaceAspects.
func (s *Server) GetInterestingPlaceAspects(
	ctx context.Context, in *pb.GetInterestingPlaceAspectsRequest) (
	*pb.GetInterestingPlaceAspectsResponse, error) {
	dcids := in.GetDcids()
	if len(dcids) == 0 {
		return nil, fmt.Errorf("missing required arguments")
	}
	if !util.CheckValidDCIDs(dcids) {
		return nil, fmt.Errorf("invalid DCIDs")
	}

	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		rowList = append(rowList, fmt.Sprintf(
			"%s%s", util.BtInterestingPlaceAspectPrefix, dcid))
	}

	dataMap, err := bigTableReadRowsParallel(ctx, s.btTable, rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			var btInterestingPlaceAspects InterestingPlaceAspects
			err := json.Unmarshal(jsonRaw, &btInterestingPlaceAspects)
			if err != nil {
				return nil, err
			}
			return &btInterestingPlaceAspects, nil
		})
	if err != nil {
		return nil, err
	}
	results := map[string]*InterestingPlaceAspects{}
	for dcid, data := range dataMap {
		results[dcid] = data.(*InterestingPlaceAspects)
	}
	jsonRaw, err := json.Marshal(results)
	if err != nil {
		return nil, err
	}
	return &pb.GetInterestingPlaceAspectsResponse{Payload: string(jsonRaw)}, nil
}
