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

// Package propertyvalues is for V2 property values API
package propertyvalues

import (
	"context"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	v1pv "github.com/datacommonsorg/mixer/internal/server/v1/propertyvalues"

	"github.com/datacommonsorg/mixer/internal/store"
)

// API is the V2 property values API implementation entry point.
func API(
	ctx context.Context,
	store *store.Store,
	nodes []string,
	properties []string,
	direction string,
	limit int,
	nextToken string,
) (*pbv2.NodeResponse, error) {
	data, _, err := v1pv.Fetch(
		ctx,
		store,
		nodes,
		properties,
		limit,
		nextToken,
		direction,
	)
	if err != nil {
		return nil, err
	}
	res := &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{}}
	for _, node := range nodes {
		res.Data[node] = &pbv2.LinkedGraph{Arcs: map[string]*pbv2.Nodes{}}
		for _, property := range properties {
			res.Data[node].Arcs[property] = &pbv2.Nodes{
				Nodes: v1pv.MergeTypedNodes(data[node][property]),
			}
		}
	}
	return res, nil
}
