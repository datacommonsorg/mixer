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

// Package observationexistence holds API implementation for observation
// existence check.
package observationexistence

import (
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"google.golang.org/protobuf/proto"

	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BulkObservationExistence implements API for Mixer.BulkObservationExistence.
func BulkObservationExistence(
	ctx context.Context,
	in *pb.BulkObservationExistenceRequest,
	store *store.Store,
) (
	*pb.BulkObservationExistenceResponse, error) {
	variables := in.GetVariables()
	entities := in.GetEntities()
	if len(variables) == 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: variables")
	}
	if len(entities) == 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: entities")
	}
	// Initialize result with "false" (non-existence).
	// The BT existence cache only returns entry when data exist.
	result := &pb.BulkObservationExistenceResponse{
		Variable: map[string]*pb.ExistenceByEntity{},
	}
	for _, v := range variables {
		result.Variable[v] = &pb.ExistenceByEntity{Entity: map[string]bool{}}
		for _, e := range entities {
			result.Variable[v].Entity[e] = false
		}
	}

	btDataList, err := bigtable.Read(
		ctx,
		store.BtGroup,
		bigtable.BtSVAndSVGExistence,
		[][]string{entities, variables},
		func(jsonRaw []byte) (interface{}, error) {
			var statVarExistence pb.EntityStatVarExistence
			if err := proto.Unmarshal(jsonRaw, &statVarExistence); err != nil {
				return nil, err
			}
			return &statVarExistence, nil
		},
	)
	if err != nil {
		return nil, err
	}
	for _, btData := range btDataList {
		for _, row := range btData {
			e := row.Parts[0]
			v := row.Parts[1]
			result.Variable[v].Entity[e] = true
		}
	}
	return result, nil
}
