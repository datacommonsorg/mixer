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

package variable

import (
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/server/statvar"
	"github.com/datacommonsorg/mixer/internal/store"
)

// Groups implements API for Mixer.VariableGroups.
func Groups(
	ctx context.Context,
	in *pb.VariableGroupsRequest,
	store *store.Store,
	cache *resource.Cache,
) (*pb.VariableGroupsResponse, error) {
	entities := in.GetEntities()
	// TODO: after deprecating v0 API, move the logic from GetStatVarGroup
	// to here direclty. Should also move the golden tests here.
	tmp, err := statvar.GetStatVarGroup(
		ctx,
		&pb.GetStatVarGroupRequest{Places: entities},
		store,
		cache,
	)
	if err != nil {
		return nil, err
	}
	return &pb.VariableGroupsResponse{Data: tmp.StatVarGroups}, nil
}
