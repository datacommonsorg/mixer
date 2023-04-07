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
	"sort"
	"strconv"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
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

// FilterSpec is a spec for filter.
type FilterSpec struct {
	Prop       string
	Unit       string
	LowerLimit float64
	UpperLimit float64
}

// Check to see if an event meets the filter criteria.
//
// This requires a specific format of the "value" field. If an event does not
// conform to the format, do not keep the event.
func keepEvent(event *pbv1.EventCollection_Event, spec FilterSpec) bool {
	if spec.Prop == "" {
		// No prop to filter by, keep event
		return true
	}
	for prop, vals := range event.GetPropVals() {
		if prop == spec.Prop {
			valStr := strings.TrimSpace(strings.TrimPrefix(vals.Vals[0], spec.Unit))
			v, err := strconv.ParseFloat(valStr, 64)
			if err != nil {
				// Can not convert the value, don't keep the event
				return false
			}
			return v >= spec.LowerLimit && v <= spec.UpperLimit
		}
	}
	// No prop found, the event does not have this prop, so not included
	return false
}

// Gets list of affected places given a place.
func getAffectedPlaces(ctx context.Context, placeDcid string, store *store.Store) ([]string, error) {
	affectedPlaces := []string{}
	if _, ok := noDataPlaces[placeDcid]; ok {
		var nodeProps []string
		if placeDcid == "Earth" {
			nodeProps = []string{"Country/typeOf", "OceanicBasin/typeOf"}
		} else { // Continent.
			// Fetch places within continents
			nodeProps = []string{placeDcid + "/containedInPlace"}
		}

		var respValues []*pb.EntityInfo
		for _, nodeProp := range nodeProps {
			resp, err := propertyvalues.PropertyValues(
				ctx,
				&pbv1.PropertyValuesRequest{
					NodeProperty: nodeProp,
					Direction:    util.DirectionIn,
				},
				store,
			)
			if err != nil {
				return nil, err
			}
			respValues = append(respValues, resp.GetValues()...)
		}

		for _, value := range respValues {
			if value.Types[0] == "Country" || value.Types[0] == "OceanicBasin" {
				affectedPlaces = append(affectedPlaces, value.Dcid)
			}
		}
	} else {
		affectedPlaces = append(affectedPlaces, placeDcid)
	}
	return affectedPlaces, nil
}

// Collection implements API for Mixer.EventCollection.
func Collection(
	ctx context.Context,
	in *pbv1.EventCollectionRequest,
	store *store.Store,
) (*pbv1.EventCollectionResponse, error) {
	if in.GetEventType() == "" || in.GetDate() == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing required arguments")
	}
	if err := util.CheckValidDCIDs([]string{in.GetAffectedPlaceDcid()}); err != nil {
		return nil, err
	}
	filterProp := in.GetFilterProp()
	filterLowerLimit := in.GetFilterLowerLimit()
	filterUpperLimit := in.GetFilterUpperLimit()
	filterUnit := in.GetFilterUnit()

	// Merge all the affected places in the final result.
	affectedPlaces, err := getAffectedPlaces(ctx, in.GetAffectedPlaceDcid(), store)
	if err != nil {
		return nil, err
	}
	btDataList, err := bigtable.Read(
		ctx,
		store.BtGroup,
		bigtable.BtEventCollection,
		[][]string{{in.GetEventType()}, affectedPlaces, {in.GetDate()}},
		func(jsonRaw []byte) (interface{}, error) {
			var eventCollection pbv1.EventCollection
			err := proto.Unmarshal(jsonRaw, &eventCollection)
			return &eventCollection, err
		},
	)
	if err != nil {
		return nil, err
	}

	resp := &pbv1.EventCollectionResponse{
		EventCollection: &pbv1.EventCollection{
			Events:         []*pbv1.EventCollection_Event{},
			ProvenanceInfo: map[string]*pbv1.EventCollection_ProvenanceInfo{},
		},
	}

	// Go through (ordered) import groups one by one, stop when data is found.
	for _, btData := range btDataList {
		if len(btData) == 0 {
			continue
		}
		// Each row represents events from a sub-place. Merge them together.
		for _, row := range btData {
			data := row.Data.(*pbv1.EventCollection)
			for _, event := range data.Events {
				if filterProp != "" {
					if !keepEvent(event, FilterSpec{
						Prop:       filterProp,
						Unit:       filterUnit,
						LowerLimit: filterLowerLimit,
						UpperLimit: filterUpperLimit,
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
	in *pbv1.EventCollectionDateRequest,
	store *store.Store,
) (*pbv1.EventCollectionDateResponse, error) {
	if in.GetEventType() == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing required arguments")
	}
	if err := util.CheckValidDCIDs([]string{in.GetAffectedPlaceDcid()}); err != nil {
		return nil, err
	}

	// Merge all the affected places in the final result.
	affectedPlaces, err := getAffectedPlaces(ctx, in.GetAffectedPlaceDcid(), store)
	if err != nil {
		return nil, err
	}

	btDataList, err := bigtable.Read(
		ctx,
		store.BtGroup,
		bigtable.BtEventCollectionDate,
		[][]string{{in.GetEventType()}, affectedPlaces},
		func(jsonRaw []byte) (interface{}, error) {
			var d pbv1.EventCollectionDate
			err := proto.Unmarshal(jsonRaw, &d)
			return &d, err
		},
	)
	if err != nil {
		return nil, err
	}

	resp := &pbv1.EventCollectionDateResponse{
		EventCollectionDate: &pbv1.EventCollectionDate{
			Dates: []string{},
		},
	}

	// Go through (ordered) import groups one by one, stop when data is found.
	for _, btData := range btDataList {
		if len(btData) == 0 {
			continue
		}
		// Each row represents events from a sub-place. Merge them together.
		dateSet := map[string]struct{}{}
		dateStrings := []string{}
		for _, row := range btData {
			data := row.Data.(*pbv1.EventCollectionDate)
			for _, date := range data.Dates {
				if _, ok := dateSet[date]; !ok {
					dateSet[date] = struct{}{}
					dateStrings = append(dateStrings, date)
				}
			}
		}
		sort.Strings(dateStrings)
		resp.EventCollectionDate.Dates = dateStrings
		break
	}

	return resp, nil
}
