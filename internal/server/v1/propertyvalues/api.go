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

package propertyvalues

import (
	"context"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PropertyValues implements mixer.PropertyValues handler.
func PropertyValues(
	ctx context.Context,
	in *pbv1.PropertyValuesRequest,
	store *store.Store,
) (*pbv1.PropertyValuesResponse, error) {
	nodeProperty := in.GetNodeProperty()
	limit := int(in.GetLimit())
	token := in.GetNextToken()
	direction := in.GetDirection()

	parts := strings.Split(nodeProperty, "/")
	if len(parts) < 2 {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid request URI")
	}
	property := parts[len(parts)-1]
	node := strings.Join(parts[0:len(parts)-1], "/")

	// Check arguments
	if property == "" {
		return nil, status.Errorf(
			codes.InvalidArgument, "missing required argument: property")
	}
	if direction != util.DirectionOut && direction != util.DirectionIn {
		return nil, status.Errorf(
			codes.InvalidArgument, "uri should be /v1/property/out/ or /v1/property/in/")
	}
	if err := util.CheckValidDCIDs([]string{node}); err != nil {
		return nil, err
	}
	data, pi, err := Fetch(
		ctx,
		store,
		[]string{node},
		[]string{property},
		limit,
		token,
		direction,
	)
	if err != nil {
		return nil, err
	}
	nextToken := ""
	if pi != nil {
		nextToken, err = util.EncodeProto(pi)
		if err != nil {
			return nil, err
		}
	}
	res := &pbv1.PropertyValuesResponse{
		NextToken: nextToken,
		Values:    MergeTypedNodes(data[node][property]),
	}
	return res, nil
}

// BulkPropertyValues implements mixer.BulkPropertyValues handler.
func BulkPropertyValues(
	ctx context.Context,
	in *pbv1.BulkPropertyValuesRequest,
	store *store.Store,
) (*pbv1.BulkPropertyValuesResponse, error) {
	property := in.GetProperty()
	nodes := in.GetNodes()
	limit := int(in.GetLimit())
	reqToken := in.GetNextToken()
	direction := in.GetDirection()

	// Check arguments
	if property == "" {
		return nil, status.Errorf(
			codes.InvalidArgument, "missing required argument: property")
	}
	if direction != util.DirectionOut && direction != util.DirectionIn {
		return nil, status.Errorf(
			codes.InvalidArgument, "uri should be /v1/bulk/property/out/** or /v1/bulk/property/in/**")
	}
	if err := util.CheckValidDCIDs(nodes); err != nil {
		return nil, err
	}
	data, pi, err := Fetch(
		ctx,
		store,
		nodes,
		[]string{property},
		limit,
		reqToken,
		direction,
	)
	if err != nil {
		return nil, err
	}
	respToken := ""
	if pi != nil {
		respToken, err = util.EncodeProto(pi)
		if err != nil {
			return nil, err
		}
	}
	res := &pbv1.BulkPropertyValuesResponse{
		NextToken: respToken,
	}
	for _, n := range nodes {
		res.Data = append(
			res.Data,
			&pb.NodePropertyValues{
				Node:   n,
				Values: MergeTypedNodes(data[n][property]),
			},
		)
	}
	return res, nil
}
