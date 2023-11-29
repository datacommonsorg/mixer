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
	"strings"

	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/cache"
	"github.com/datacommonsorg/mixer/internal/server/statvar/hierarchy"
	"github.com/datacommonsorg/mixer/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Ancestors implements API for Mixer.VariableAncestors.
func Ancestors(
	ctx context.Context,
	in *pbv1.VariableAncestorsRequest,
	store *store.Store,
	cachedata *cache.Cache,
) (*pbv1.VariableAncestorsResponse, error) {
	node := in.GetNode()
	if node == "" {
		return nil, status.Errorf(
			codes.InvalidArgument, "Missing required argument: node")
	}
	ancestors := []string{}
	curr := node
	for {
		if parents, ok := cachedata.ParentSvgs()[curr]; ok {
			curr = parents[0]
			for _, parent := range parents {
				// Prefer parent from custom import group
				if strings.HasPrefix(parent, "dc/g/Custom_") {
					curr = parent
					break
				}
			}
			if curr == hierarchy.SvgRoot {
				break
			}
			ancestors = append(ancestors, curr)
		} else {
			break
		}
	}
	return &pbv1.VariableAncestorsResponse{
		Ancestors: ancestors,
	}, nil
}
