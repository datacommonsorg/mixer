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

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/place"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PlaceInfo implements API for Mixer.PlaceInfo.
func PlaceInfo(
	ctx context.Context,
	in *pb.PlaceInfoRequest,
	store *store.Store,
) (*pb.PlaceInfoResponse, error) {
	node := in.GetNode()
	if !util.CheckValidDCIDs([]string{node}) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid node")
	}

	placeToMetadata, err := place.GetPlaceMetadataHelper(ctx, []string{node}, store)
	if err != nil {
		return nil, err
	}

	resp := &pb.PlaceInfoResponse{Node: node}
	if metadata, ok := placeToMetadata[node]; ok {
		resp.Info = metadata
	}

	return resp, nil
}

// BulkPlaceInfo implements API for Mixer.BulkPlaceInfo.
func BulkPlaceInfo(
	ctx context.Context,
	in *pb.BulkPlaceInfoRequest,
	store *store.Store,
) (*pb.BulkPlaceInfoResponse, error) {
	nodes := in.GetNodes()
	if len(nodes) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Missing required arguments: nodes")
	}
	if !util.CheckValidDCIDs(nodes) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid nodes")
	}

	placeToMetadata, err := place.GetPlaceMetadataHelper(ctx, nodes, store)
	if err != nil {
		return nil, err
	}

	resp := &pb.BulkPlaceInfoResponse{}
	for _, node := range nodes {
		item := &pb.PlaceInfoResponse{Node: node}
		if metadata, ok := placeToMetadata[node]; ok {
			item.Info = metadata
		}
		resp.Data = append(resp.Data, item)
	}

	return resp, nil
}
