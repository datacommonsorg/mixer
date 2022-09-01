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

package internalplace

import (
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/placepage"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetPlacePageData implements API for Mixer.GetPlacePageData.
func GetPlacePageData(
	ctx context.Context, in *pb.GetPlacePageDataRequest, store *store.Store,
) (*pb.GetPlacePageDataResponse, error) {
	placeDcid := in.GetPlace()
	if !util.CheckValidDCIDs([]string{placeDcid}) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid place")
	}
	seed := in.GetSeed()
	newStatVars := in.GetNewStatVars()
	return placepage.GetPlacePageDataHelper(ctx, placeDcid, newStatVars, seed, store, "")
}
