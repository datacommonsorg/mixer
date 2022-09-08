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

package page

import (
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/placepage"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PlacePage implements API for Mixer.PlacePage.
func PlacePage(
	ctx context.Context,
	in *pb.PlacePageRequest,
	store *store.Store,
) (*pb.GetPlacePageDataResponse, error) {
	node := in.GetNode()
	if !util.CheckValidDCIDs([]string{node}) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid entity")
	}
	seed := in.GetSeed()
	category := in.GetCategory()
	newStatVars := in.GetNewStatVars()
	return placepage.GetPlacePageDataHelper(ctx, node, newStatVars, seed, store, category)
}
