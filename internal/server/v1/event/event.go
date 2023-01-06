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

// Package event contain code for event.
package event

import (
	"context"
	"strconv"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/v1/propertyvalues"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var noDataPlaces = map[string]struct{}{
	"Earth":        {},
	"africa":       {},
	"antarctica":   {},
	"asia":         {},
	"europe":       {},
	"northamerica": {},
	"oceania":      {},
	"southamerica": {},
}

type filterSpec struct {
	prop       string
	unit       string
	lowerLimit float64
	upperLimit float64
}

// Check to see if an even meets the filter criteria.
//
// This requires a specific format of the "value" field. If an event does not
// conform to the format, keep it for now until we can handle all the format
// properly.
func keepEvent(event *pb.EventCollection_Event, spec filterSpec) bool {
	for prop, vals := range event.GetPropVals() {
		if prop == spec.prop {
			valStr := strings.TrimPrefix(vals.Vals[0], spec.unit)
			v, err := strconv.ParseFloat(valStr, 64)
			if err != nil {
				// Can not convert the value, keep.
				return true
			}
			return v >= spec.lowerLimit && v <= spec.upperLimit
		}
	}
	// No prop found, keep event
	return true
}

// Collection implements API for Mixer.EventCollection.
func Collection(
	ctx context.Context,
	in *pb.EventCollectionRequest,
	store *store.Store,
) (*pb.EventCollectionResponse, error) {
	if in.GetEventType() == "" || in.GetDate() == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing required arguments")
	}
	if !util.CheckValidDCIDs([]string{in.GetAffectedPlaceDcid()}) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid DCID")
	}
	filterProp := in.GetFilterProp()
	filterLowerLimit := in.GetFilterLowerLimit()
	filterUpperLimit := in.GetFilterUpperLimit()
	filterUnit := in.GetFilterUnit()

	// Merge all the affected places in the final result.
	affectedPlaces := []string{}
	if _, ok := noDataPlaces[in.GetAffectedPlaceDcid()]; ok {
		nodeProp := "Country/typeOf"
		// Fetch places within continents
		if in.GetAffectedPlaceDcid() != "Earth" {
			nodeProp = in.GetAffectedPlaceDcid() + "/containedInPlace"
		}
		resp, err := propertyvalues.PropertyValues(
			ctx,
			&pb.PropertyValuesRequest{
				NodeProperty: nodeProp,
				Direction:    util.DirectionIn,
			},
			store,
		)
		if err != nil {
			return nil, err
		}
		for _, value := range resp.Values {
			if value.Types[0] == "Country" {
				affectedPlaces = append(affectedPlaces, value.Dcid)
			}
		}
	} else {
		affectedPlaces = append(affectedPlaces, in.GetAffectedPlaceDcid())
	}

	btDataList, err := bigtable.Read(
		ctx,
		store.BtGroup,
		bigtable.BtEventCollection,
		[][]string{{in.GetEventType()}, affectedPlaces, {in.GetDate()}},
		func(jsonRaw []byte) (interface{}, error) {
			var eventCollection pb.EventCollection
			err := proto.Unmarshal(jsonRaw, &eventCollection)
			return &eventCollection, err
		},
	)
	if err != nil {
		return nil, err
	}

	resp := &pb.EventCollectionResponse{
		EventCollection: &pb.EventCollection{
			Events:         []*pb.EventCollection_Event{},
			ProvenanceInfo: map[string]*pb.EventCollection_ProvenanceInfo{},
		},
	}

	// Go through (ordered) import groups one by one, stop when data is found.
	for _, btData := range btDataList {
		if len(btData) == 0 {
			continue
		}
		// Each row represents events from a sub-place. Merge them together.
		for _, row := range btData {
			data := row.Data.(*pb.EventCollection)
			for _, event := range data.Events {
				if filterProp != "" {
					if !keepEvent(event, filterSpec{
						prop:       filterProp,
						unit:       filterUnit,
						lowerLimit: filterLowerLimit,
						upperLimit: filterUpperLimit,
					}) {
						continue
					}
				}
				resp.EventCollection.Events = append(
					resp.EventCollection.Events,
					event,
				)
				provID := event.ProvenanceId
				resp.EventCollection.ProvenanceInfo[provID] = data.ProvenanceInfo[provID]
			}
		}
		break
	}
	return resp, nil
}

// CollectionDate implements API for Mixer.EventCollectionDate.
func CollectionDate(
	ctx context.Context,
	in *pb.EventCollectionDateRequest,
	store *store.Store,
) (*pb.EventCollectionDateResponse, error) {
	if in.GetEventType() == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing required arguments")
	}
	if !util.CheckValidDCIDs([]string{in.GetAffectedPlaceDcid()}) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid DCID")
	}

	btDataList, err := bigtable.Read(
		ctx,
		store.BtGroup,
		bigtable.BtEventCollectionDate,
		[][]string{{in.GetEventType()}, {in.GetAffectedPlaceDcid()}},
		func(jsonRaw []byte) (interface{}, error) {
			var d pb.EventCollectionDate
			err := proto.Unmarshal(jsonRaw, &d)
			return &d, err
		},
	)
	if err != nil {
		return nil, err
	}

	resp := &pb.EventCollectionDateResponse{}

	// Go through (ordered) import groups one by one, stop when data is found.
	for _, btData := range btDataList {
		for _, row := range btData {
			resp.EventCollectionDate = row.Data.(*pb.EventCollectionDate)
			break
		}
	}

	return resp, nil
}
