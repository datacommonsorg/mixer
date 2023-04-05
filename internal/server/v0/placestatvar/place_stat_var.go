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

package placestatvar

import (
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/statvar"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetPlaceStatVars implements API for Mixer.GetPlaceStatVars.
func GetPlaceStatVars(
	ctx context.Context, in *pb.GetPlaceStatVarsRequest, store *store.Store) (
	*pb.GetPlaceStatVarsResponse, error) {
	dcids := in.GetDcids()
	if len(dcids) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Missing required arguments: dcids")
	}
	if err := util.CheckValidDCIDs(dcids); err != nil {
		return nil, err
	}
	entityToStatVars, err := statvar.GetEntityStatVarsHelper(ctx, dcids, store)
	if err != nil {
		return nil, err
	}

	resp := &pb.GetPlaceStatVarsResponse{Places: map[string]*pb.StatVars{}}
	for entity, statVars := range entityToStatVars {
		resp.Places[entity] = statVars
	}

	return resp, nil
}

// GetEntityStatVarsUnionV1 implements API for Mixer.GetEntityStatVarsUnionV1.
func GetEntityStatVarsUnionV1(
	ctx context.Context, in *pb.GetEntityStatVarsUnionRequest, store *store.Store,
) (*pb.GetEntityStatVarsUnionResponse, error) {
	return nil, nil
}
