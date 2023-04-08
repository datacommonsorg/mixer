// Copyright 2023 Google LLC
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

// Package properties is for V2 event API.
package event

import (
	"context"
	"strconv"
	"strings"

	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	v1e "github.com/datacommonsorg/mixer/internal/server/v1/event"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/datacommonsorg/mixer/internal/store"
)

// EventCollection implements for EventCollection.
func EventCollection(
	ctx context.Context,
	store *store.Store,
	node string,
	eventType string,
	date string,
	filter *v1e.FilterSpec,
) (*pbv2.EventResponse, error) {
	req := &pbv1.EventCollectionRequest{
		EventType:         eventType,
		AffectedPlaceDcid: node,
		Date:              date,
	}
	if filter != nil {
		req.FilterProp = filter.Prop
		req.FilterUnit = filter.Unit
		req.FilterLowerLimit = filter.LowerLimit
		req.FilterUpperLimit = filter.UpperLimit
	}
	data, err := v1e.Collection(ctx, req, store)
	if err != nil {
		return nil, err
	}
	return &pbv2.EventResponse{
		EventCollection: data.GetEventCollection(),
	}, nil
}

// EventCollectionDate implements for EventCollectionDate.
func EventCollectionDate(
	ctx context.Context,
	store *store.Store,
	node string,
	eventType string,
) (*pbv2.EventResponse, error) {
	data, err := v1e.CollectionDate(ctx,
		&pbv1.EventCollectionDateRequest{
			EventType:         eventType,
			AffectedPlaceDcid: node,
		},
		store)
	if err != nil {
		return nil, err
	}
	return &pbv2.EventResponse{
		EventCollectionDate: data.GetEventCollectionDate(),
	}, nil
}

// ParseEventCollectionFilter parses filter for EventCollection.
// Example: property = "area", filterExpr = "3.1#6.2#Acre".
func ParseEventCollectionFilter(property, filterExpr string) (*v1e.FilterSpec, error) {
	if property == "" || filterExpr == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"invalid event filter: %s, %s", property, filterExpr)
	}

	parts := strings.Split(filterExpr, "#")
	if len(parts) != 3 {
		return nil, status.Errorf(codes.InvalidArgument,
			"invalid event filter: %s", filterExpr)
	}

	var err error
	res := &v1e.FilterSpec{Prop: property, Unit: parts[2]}

	res.LowerLimit, err = strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument,
			"invalid event filter lower limit: %s", filterExpr)
	}

	res.UpperLimit, err = strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument,
			"invalid event filter upper limit: %s", filterExpr)
	}

	return res, nil
}
