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

package info

import (
	"context"

	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/place"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PlaceInfo implements API for Mixer.PlaceInfo.
func PlaceInfo(
	ctx context.Context,
	in *pbv1.PlaceInfoRequest,
	store *store.Store,
) (*pbv1.PlaceInfoResponse, error) {
	node := in.GetNode()
	if err := util.CheckValidDCIDs([]string{node}); err != nil {
		return nil, err
	}
	placeToMetadata, err := place.GetPlaceMetadataHelper(ctx, []string{node}, store)
	if err != nil {
		return nil, err
	}

	resp := &pbv1.PlaceInfoResponse{Node: node}
	if metadata, ok := placeToMetadata[node]; ok {
		resp.Info = metadata
	}

	return resp, nil
}

// BulkPlaceInfo implements API for Mixer.BulkPlaceInfo.
func BulkPlaceInfo(
	ctx context.Context,
	in *pbv1.BulkPlaceInfoRequest,
	store *store.Store,
) (*pbv1.BulkPlaceInfoResponse, error) {
	nodes := in.GetNodes()
	if len(nodes) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Missing required arguments: nodes")
	}
	if err := util.CheckValidDCIDs(nodes); err != nil {
		return nil, err
	}
	placeToMetadata, err := place.GetPlaceMetadataHelper(ctx, nodes, store)
	if err != nil {
		return nil, err
	}

	resp := &pbv1.BulkPlaceInfoResponse{}
	for _, node := range nodes {
		item := &pbv1.PlaceInfoResponse{Node: node}
		if metadata, ok := placeToMetadata[node]; ok {
			item.Info = metadata
		}
		resp.Data = append(resp.Data, item)
	}

	return resp, nil
}
