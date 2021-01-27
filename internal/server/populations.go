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
	"log"
	"sort"

	"cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// PopObs represents a pair of population and observation node.
type PopObs struct {
	PopulationID     string `json:"dcid,omitempty"`
	ObservationValue string `json:"observation,omitempty"`
}

// GetPopObs implements API for Mixer.GetPopObs.
func (s *Server) GetPopObs(ctx context.Context, in *pb.GetPopObsRequest) (
	*pb.GetPopObsResponse, error) {
	if s.store.BaseBt() == nil || s.store.BranchBt() == nil {
		return nil, status.Errorf(
			codes.NotFound, "Bigtable instance is not specified")
	}
	dcid := in.GetDcid()

	if dcid == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing argument: dcid")
	}
	if !util.CheckValidDCIDs([]string{dcid}) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid DCIDs: %s", dcid)
	}

	rowList := bigtable.RowList{util.BtPopObsPrefix + dcid}
	baseDataMap, branchDataMap, err := bigTableReadRowsParallel(
		ctx, s.store, rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			popObsCache := &pb.PopObsPlace{}
			err := protojson.Unmarshal(jsonRaw, popObsCache)
			if err != nil {
				return nil, err
			}
			return popObsCache, nil
		}, nil)
	if err != nil {
		return nil, err
	}
	baseData := &pb.PopObsPlace{}
	var branchData *pb.PopObsPlace
	if baseDataMap[dcid] != nil {
		baseData = baseDataMap[dcid].(*pb.PopObsPlace)
	}
	if branchDataMap[dcid] != nil {
		branchData = branchDataMap[dcid].(*pb.PopObsPlace)
	}
	if branchData != nil {
		for k, v := range branchData.Populations {
			baseData.Populations[k] = v
		}
	}
	resStr, err := protojson.Marshal(baseData)
	if err != nil {
		return nil, err
	}
	out := pb.GetPopObsResponse{}
	out.Payload, err = util.ZipAndEncode([]byte(resStr))
	return &out, err
}

// GetPlaceObs implements API for Mixer.GetPlaceObs.
func (s *Server) GetPlaceObs(ctx context.Context, in *pb.GetPlaceObsRequest) (
	*pb.GetPlaceObsResponse, error) {
	if s.store.BaseBt() == nil || s.store.BranchBt() == nil {
		return nil, status.Errorf(
			codes.NotFound, "Bigtable instance is not specified")
	}
	if in.GetPlaceType() == "" || in.GetPopulationType() == "" ||
		in.GetObservationDate() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "missing required arguments")
	}

	key := fmt.Sprintf("%s^%s^%s", in.GetPlaceType(), in.GetObservationDate(),
		in.GetPopulationType())
	if len(in.GetPvs()) > 0 {
		iterateSortPVs(in.GetPvs(), func(i int, p, v string) {
			key += "^" + p + "^" + v
		})
	}
	key = fmt.Sprintf("%s%s", util.BtPlaceObsPrefix, key)
	out := pb.GetPlaceObsResponse{}

	// TODO(boxu): abstract out the common logic for handling cache merging.
	baseData := &pb.PopObsCollection{}
	branchData := &pb.PopObsCollection{}
	var baseRaw, branchRaw []byte
	var hasBaseData, hasBranchData bool
	out.Payload, _ = util.ZipAndEncode([]byte("{}"))

	btRow, err := s.store.BranchBt().ReadRow(ctx, key)
	if err != nil {
		log.Print(err)
	}
	hasBranchData = len(btRow[util.BtFamily]) > 0
	if hasBranchData {
		branchRaw = btRow[util.BtFamily][0].Value
	}

	btRow, err = s.store.BaseBt().ReadRow(ctx, key)
	if err != nil {
		log.Print(err)
	}
	hasBaseData = len(btRow[util.BtFamily]) > 0
	if hasBaseData {
		baseRaw = btRow[util.BtFamily][0].Value
	}

	if !hasBaseData && !hasBranchData {
		return &out, nil
	} else if !hasBaseData {
		out.Payload = string(branchRaw)
		return &out, nil
	} else if !hasBranchData {
		out.Payload = string(baseRaw)
		return &out, nil
	} else {
		if tmp, err := util.UnzipAndDecode(string(baseRaw)); err == nil {
			err := protojson.Unmarshal(tmp, baseData)
			if err != nil {
				return nil, err
			}
		}
		if tmp, err := util.UnzipAndDecode(string(branchRaw)); err == nil {
			err := protojson.Unmarshal(tmp, branchData)
			if err != nil {
				return nil, err
			}
		}
		dataMap := map[string]*pb.PopObsPlace{}
		for _, data := range baseData.Places {
			dataMap[data.Place] = data
		}
		for _, data := range branchData.Places {
			dataMap[data.Place] = data
		}
		res := &pb.PopObsCollection{}
		for _, v := range dataMap {
			res.Places = append(res.Places, v)
		}
		resBytes, err := protojson.Marshal(res)
		if err != nil {
			return &out, err
		}
		out.Payload, err = util.ZipAndEncode(resBytes)
		return &out, err
	}
}

// GetPopulations implements API for Mixer.GetPopulations.
func (s *Server) GetPopulations(
	ctx context.Context, in *pb.GetPopulationsRequest) (
	*pb.GetPopulationsResponse, error) {

	dcids := in.GetDcids()

	if len(dcids) == 0 || in.GetPopulationType() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Missing required arguments")
	}
	if !util.CheckValidDCIDs(dcids) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid DCIDs")
	}

	// Create the cache key suffix
	keySuffix := "^" + in.GetPopulationType()
	if len(in.GetPvs()) > 0 {
		iterateSortPVs(in.GetPvs(), func(i int, p, v string) {
			keySuffix += ("^" + p + "^" + v)
		})
	}

	// Generate the list of all keys to query cache for
	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		btKey := util.BtPopPrefix + dcid + keySuffix
		rowList = append(rowList, btKey)
	}

	// Query the cache
	collection := []*PlacePopInfo{}
	baseDataMap, branchDataMap, err := bigTableReadRowsParallel(ctx, s.store, rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			return string(jsonRaw), nil
		}, nil)
	if err != nil {
		return nil, err
	}
	for _, dcid := range dcids {
		item := &PlacePopInfo{}
		if data, ok := baseDataMap[dcid]; ok {
			item = &PlacePopInfo{
				PlaceID:      dcid,
				PopulationID: data.(string),
			}
		} else if data, ok := branchDataMap[dcid]; ok {
			item = &PlacePopInfo{
				PlaceID:      dcid,
				PopulationID: data.(string),
			}
		}
		collection = append(collection, item)
	}
	// Format the response
	jsonRaw, err := json.Marshal(collection)
	if err != nil {
		return nil, err
	}
	return &pb.GetPopulationsResponse{Payload: string(jsonRaw)}, nil
}

// GetObservations implements API for Mixer.GetObservations.
func (s *Server) GetObservations(
	ctx context.Context, in *pb.GetObservationsRequest) (
	*pb.GetObservationsResponse, error) {
	dcids := in.GetDcids()
	// TODO: Add checks for empty in.GetStatType().
	if len(dcids) == 0 || in.GetMeasuredProperty() == "" ||
		in.GetObservationDate() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Missing required arguments")
	}
	if !util.CheckValidDCIDs(dcids) {
		return nil, status.Error(codes.InvalidArgument, "Invalid DCIDs")
	}

	// Construct the list of cache keys to query.
	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		btKey := fmt.Sprintf("%s%s^%s^%s^%s^%s^%s^^^",
			util.BtObsPrefix, dcid, in.GetMeasuredProperty(),
			util.SnakeToCamel(in.GetStatsType()), in.GetObservationDate(),
			in.GetObservationPeriod(), in.GetMeasurementMethod())
		rowList = append(rowList, btKey)
	}

	// Query the cache for all keys.
	collection := []*PopObs{}
	baseDataMap, branchDataMap, err := bigTableReadRowsParallel(
		ctx, s.store, rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			return string(jsonRaw), nil
		}, nil)
	if err != nil {
		return nil, err
	}

	for _, dcid := range dcids {
		if obs, ok := baseDataMap[dcid]; ok {
			item := &PopObs{
				PopulationID:     dcid,
				ObservationValue: obs.(string),
			}
			collection = append(collection, item)
		} else if obs, ok := branchDataMap[dcid]; ok {
			item := &PopObs{
				PopulationID:     dcid,
				ObservationValue: obs.(string),
			}
			collection = append(collection, item)
		}
	}

	// Format the response
	jsonRaw, err := json.Marshal(collection)
	if err != nil {
		return nil, err
	}
	return &pb.GetObservationsResponse{Payload: string(jsonRaw)}, nil

}

// iterateSortPVs iterates a list of PVs and performs actions on them.
func iterateSortPVs(pvs []*pb.PropertyValue, action func(i int, p, v string)) {
	pvMap := map[string]string{}
	pList := []string{}
	for _, pv := range pvs {
		pvMap[pv.GetProperty()] = pv.GetValue()
		pList = append(pList, pv.GetProperty())
	}
	sort.Strings(pList)
	for i, p := range pList {
		action(i, p, pvMap[p])
	}
}
