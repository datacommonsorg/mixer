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

	pb "github.com/datacommonsorg/mixer/internal/proto"
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
) (*pb.QueryV2Response, error) {
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
	res := &pb.QueryV2Response{Data: map[string]*pb.Arc{}}
	for _, node := range nodes {
		for _, property := range properties {
			res.Data[node] = &pb.Arc{
				Property: property,
				Nodes:    v1pv.MergeTypedNodes(data[node][property]),
			}
		}
	}
	return res, nil
}
