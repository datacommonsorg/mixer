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

// Package properties is for V2 properties API.
package properties

import (
	"context"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	nodewrapper "github.com/datacommonsorg/mixer/internal/server/node"
	"github.com/datacommonsorg/mixer/internal/util"

	"github.com/datacommonsorg/mixer/internal/store"
)

// API is the V2 properties API implementation entry point.
func API(
	ctx context.Context,
	store *store.Store,
	nodes []string,
	direction string,
) (*pbv2.NodeResponse, error) {
	data, err := nodewrapper.GetPropertiesHelper(ctx, nodes, store)
	if err != nil {
		return nil, err
	}
	res := &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{}}
	if direction == util.DirectionIn {
		for node, d := range data {
			res.Data[node] = &pbv2.LinkedGraph{
				Properties: d.GetInLabels(),
			}
		}
	} else if direction == util.DirectionOut {
		for node, d := range data {
			res.Data[node] = &pbv2.LinkedGraph{
				Properties: d.GetOutLabels(),
			}
		}
	}
	return res, nil
}
