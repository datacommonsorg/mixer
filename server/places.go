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
		}, nil)
	if err != nil {
		return nil, err
	}
	results := []map[string]string{}
	for _, dcid := range dcids {
		if dataMap[dcid] != nil {
			for _, place := range dataMap[dcid].([]string) {
				results = append(results, map[string]string{"dcid": dcid, "place": place})
			}
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

	measuredProperty := in.GetMeasuredProperty()
	popObsSignatureItems := []string{
		measuredProperty,
		"",
		in.GetMeasurementDenominator(),
		in.GetMeasurementQualifier(),
		"",
		"",
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
		}, nil)
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

// RelatedLocationsPrefixMap is a map from different scenarios to key prefix for
// RelatedLocations cache.
//
// The three levels of keys are:
// - Whether related locaitons have the same ancestor.
// - Whether related locaitons have the same place type.
// - Whether closeness computaion is per capita.
var RelatedLocationsPrefixMap = map[bool]map[bool]map[bool]string{
	true: {
		true: {
			true:  util.BtRelatedLocationsSameTypeAndAncestorPCPrefix,
			false: util.BtRelatedLocationsSameTypeAndAncestorPrefix,
		},
		false: {
			true:  util.BtRelatedLocationsSameAncestorPCPrefix,
			false: util.BtRelatedLocationsSameAncestorPrefix,
		},
	},
	false: {
		true: {
			true:  util.BtRelatedLocationsSameTypePCPrefix,
			false: util.BtRelatedLocationsSameTypePrefix,
		},
		false: {
			true:  util.BtRelatedLocationsPCPrefix,
			false: util.BtRelatedLocationsPrefix,
		},
	},
}

// GetRelatedLocations implements API for Mixer.GetRelatedLocations.
func (s *Server) GetRelatedLocations(ctx context.Context,
	in *pb.GetRelatedLocationsRequest) (*pb.GetRelatedLocationsResponse, error) {
	if in.GetDcid() == "" || len(in.GetStatVarDcids()) == 0 {
		return nil, fmt.Errorf("missing required arguments")
	}
	if !util.CheckValidDCIDs([]string{in.GetDcid()}) {
		return nil, fmt.Errorf("invalid DCID")
	}

	sameAncestor := (in.GetWithinPlace() != "")
	samePlaceType := in.GetSamePlaceType()
	isPerCapita := in.GetIsPerCapita()
	prefix := RelatedLocationsPrefixMap[sameAncestor][samePlaceType][isPerCapita]

	rowList := bigtable.RowList{}
	for _, statVarDcid := range in.GetStatVarDcids() {
		if sameAncestor {
			rowList = append(rowList, fmt.Sprintf(
				"%s%s^%s^%s", prefix, in.GetDcid(), in.GetWithinPlace(), statVarDcid))
		} else {
			rowList = append(rowList, fmt.Sprintf(
				"%s%s^%s", prefix, in.GetDcid(), statVarDcid))
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
		}, func(key string) (string, error) {
			parts := strings.Split(key, "^")
			if len(parts) <= 1 {
				return "", fmt.Errorf("no ^ in key to parse for StatVarDcid")
			}
			return parts[len(parts)-1], nil
		})
	if err != nil {
		return nil, err
	}
	results := map[string]*RelatedPlacesInfo{}
	for statVarDcid, data := range dataMap {
		results[statVarDcid] = data.(*RelatedPlacesInfo)
	}
	jsonRaw, err := json.Marshal(results)
	if err != nil {
		return nil, err
	}
	return &pb.GetRelatedLocationsResponse{Payload: string(jsonRaw)}, nil
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
		}, nil)
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

// GetPlaceStatsVar implements API for Mixer.GetPlaceStatsVar.
func (s *Server) GetPlaceStatsVar(
	ctx context.Context, in *pb.GetPlaceStatsVarRequest) (
	*pb.GetPlaceStatsVarResponse, error) {
	dcids := in.GetDcids()
	if len(dcids) == 0 {
		return nil, fmt.Errorf("Missing required arguments: dcid")
	}
	rowList := buildPlaceStatsVarKey(dcids)
	dataMap, err := bigTableReadRowsParallel(ctx, s.btTable, rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			var data PlaceStatsVar
			err := json.Unmarshal(jsonRaw, &data)
			if err != nil {
				return nil, err
			}
			return data.StatVarIds, nil
		}, nil)
	if err != nil {
		return nil, err
	}
	resp := pb.GetPlaceStatsVarResponse{Places: map[string]*pb.StatsVars{}}
	for _, dcid := range dcids {
		resp.Places[dcid] = &pb.StatsVars{StatsVars: []string{}}
		if dataMap[dcid] != nil {
			resp.Places[dcid].StatsVars = dataMap[dcid].([]string)
		}
	}
	return &resp, nil
}

// GetLandingPage implements API for Mixer.GetLandingPage.
func (s *Server) GetLandingPage(
	ctx context.Context, in *pb.GetLandingPageRequest) (
	*pb.GetLandingPageResponse, error) {
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
			"%s%s", util.BtLandingPagePrefix, dcid))
	}

	dataMap, err := bigTableReadRowsParallel(ctx, s.btTable, rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			var btLandingPageInfo LandingPageInfo
			err := json.Unmarshal(jsonRaw, &btLandingPageInfo)
			if err != nil {
				return nil, err
			}
			return &btLandingPageInfo, nil
		}, nil)
	if err != nil {
		return nil, err
	}
	results := map[string]*LandingPageInfo{}
	for dcid, data := range dataMap {
		results[dcid] = data.(*LandingPageInfo)
	}
	jsonRaw, err := json.Marshal(results)
	if err != nil {
		return nil, err
	}
	return &pb.GetLandingPageResponse{Payload: string(jsonRaw)}, nil
}
