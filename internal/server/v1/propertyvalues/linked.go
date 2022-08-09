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

package propertyvalues

import (
	"context"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/placein"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LinkedPropertyValues implements mixer.LinkedPropertyValues handler.
func LinkedPropertyValues(
	ctx context.Context,
	in *pb.LinkedPropertyValuesRequest,
	store *store.Store,
) (*pb.PropertyValuesResponse, error) {
	entityProperty := in.GetEntityProperty()
	parts := strings.Split(entityProperty, "/")
	if len(parts) < 2 {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid request URI")
	}
	property := parts[len(parts)-1]
	entity := strings.Join(parts[0:len(parts)-1], "/")
	valueEntityType := in.GetValueEntityType()
	// Check arguments
	if property != "containedInPlace" {
		return nil, status.Errorf(
			codes.InvalidArgument, "only support property 'containedInPlace'")
	}
	if valueEntityType == "" {
		return nil, status.Errorf(
			codes.InvalidArgument, "missing argument: value_entity_type")
	}
	if !util.CheckValidDCIDs([]string{entity}) {
		return nil, status.Errorf(
			codes.InvalidArgument, "invalid entity %s", entity)
	}
	resp, err := placein.GetPlacesIn(
		ctx,
		store,
		[]string{entity},
		valueEntityType,
	)
	if err != nil {
		return nil, err
	}
	valueDcids := resp[entity]
	// Fetch names
	data, _, err := Fetch(
		ctx,
		store,
		[]string{"name"},
		valueDcids,
		0,
		"",
		"out",
	)
	if err != nil {
		return nil, err
	}
	result := &pb.PropertyValuesResponse{}
	for _, dcid := range valueDcids {
		var name string
		if tmp, ok := data["name"][dcid]; ok {
			name = tmp[""][0].Value
		}
		result.Values = append(result.Values,
			&pb.EntityInfo{
				Dcid: dcid,
				Name: name,
			},
		)
	}
	return result, nil
}

// BulkLinkedPropertyValues implements mixer.BulkLinkedPropertyValues handler.
func BulkLinkedPropertyValues(
	ctx context.Context,
	in *pb.BulkLinkedPropertyValuesRequest,
	store *store.Store,
) (*pb.BulkPropertyValuesResponse, error) {
	property := in.GetProperty()
	entities := in.GetEntities()
	valueEntityType := in.GetValueEntityType()
	// Check arguments
	if property != "containedInPlace" {
		return nil, status.Errorf(
			codes.InvalidArgument, "only support property 'containedInPlace'")
	}
	if valueEntityType == "" {
		return nil, status.Errorf(
			codes.InvalidArgument, "missing argument: value_entity_type")
	}
	if !util.CheckValidDCIDs(entities) {
		return nil, status.Errorf(
			codes.InvalidArgument, "invalid entities %s", entities)
	}
	resp, err := placein.GetPlacesIn(
		ctx,
		store,
		entities,
		valueEntityType,
	)
	if err != nil {
		return nil, err
	}
	valueDcids := []string{}
	for _, e := range resp {
		valueDcids = append(valueDcids, e...)
	}
	// Fetch names
	data, _, err := Fetch(
		ctx,
		store,
		[]string{"name"},
		valueDcids,
		0,
		"",
		"out",
	)
	if err != nil {
		return nil, err
	}
	result := &pb.BulkPropertyValuesResponse{}
	for _, e := range entities {
		children := resp[e]
		oneEntityResult := &pb.BulkPropertyValuesResponse_EntityPropertyValues{
			Entity: e,
		}
		for _, dcid := range children {
			var name string
			if nameValues, ok := data["name"][dcid]; ok {
				name = nameValues[""][0].Value
			}
			oneEntityResult.Values = append(oneEntityResult.Values,
				&pb.EntityInfo{
					Dcid: dcid,
					Name: name,
				},
			)
		}
		result.Data = append(result.Data, oneEntityResult)
	}
	return result, nil
}
