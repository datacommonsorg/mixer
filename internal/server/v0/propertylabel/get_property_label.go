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

package propertylabel

import (
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/node"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetPropertyLabels implements API for Mixer.GetPropertyLabels.
func GetPropertyLabels(
	ctx context.Context,
	in *pb.GetPropertyLabelsRequest,
	store *store.Store,
) (*pb.GetPropertyLabelsResponse, error) {
	dcids := in.GetDcids()
	if len(dcids) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Missing required arguments: dcids")
	}
	if err := util.CheckValidDCIDs(dcids); err != nil {
		return nil, err
	}
	outProps, err := node.GetPropertiesHelper(ctx, dcids, store, util.DirectionOut)
	if err != nil {
		return nil, err
	}
	inProps, err := node.GetPropertiesHelper(ctx, dcids, store, util.DirectionIn)
	if err != nil {
		return nil, err
	}
	result := &pb.GetPropertyLabelsResponse{Data: map[string]*pb.PropertyLabels{}}

	init := func(entity string) {
		if _, ok := result.Data[entity]; !ok {
			result.Data[entity] = &pb.PropertyLabels{
				InLabels:  []string{},
				OutLabels: []string{},
			}
		}
	}
	for entity, props := range outProps {
		init(entity)
		result.Data[entity].OutLabels = props
	}
	for entity, props := range inProps {
		init(entity)
		result.Data[entity].InLabels = props
	}
	return result, nil
}
