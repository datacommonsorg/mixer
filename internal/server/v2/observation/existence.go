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

// Package observation is for V2 observation API
package observation

import (
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"google.golang.org/protobuf/proto"
)

// Existence implements logic to check existence for entity, variable pair.
func Existence(
	ctx context.Context,
	store *store.Store,
	variables []string,
	entities []string,
) (*pbv2.ObservationResponse, error) {
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
	result := &pbv2.ObservationResponse{
		ByVariable: map[string]*pbv2.VariableObservation{},
	}
	for _, btData := range btDataList {
		for _, row := range btData {
			e := row.Parts[0]
			v := row.Parts[1]
			obsByVar := result.ByVariable // Short alias
			if _, ok := obsByVar[v]; !ok {
				obsByVar[v] = &pbv2.VariableObservation{
					ByEntity: map[string]*pbv2.EntityObservation{},
				}
			}
			obsByVar[v].ByEntity[e] = &pbv2.EntityObservation{}
		}
	}
	return result, nil
}
