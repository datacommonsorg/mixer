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

package triples

import (
	"context"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/node"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/server/v1/properties"
	"github.com/datacommonsorg/mixer/internal/server/v1/propertyvalues"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func getObsTriples(
	ctx context.Context,
	store *store.Store,
	metadata *resource.Metadata,
	dcids []string,
) ([]*pbv1.BulkTriplesResponse_NodeTriples, error) {
	resp, err := node.GetObsTriples(ctx, store, metadata, dcids)
	if err != nil {
		return nil, err
	}
	result := []*pbv1.BulkTriplesResponse_NodeTriples{}
	for dcid, tripleList := range resp {
		item := &pbv1.BulkTriplesResponse_NodeTriples{
			Node:    dcid,
			Triples: map[string]*pbv1.NodeInfoCollection{},
		}
		for _, t := range tripleList {
			item.Triples[t.Predicate] = &pbv1.NodeInfoCollection{
				Nodes: []*pb.EntityInfo{
					{
						Name:         t.ObjectName,
						Value:        t.ObjectValue,
						Types:        t.ObjectTypes,
						Dcid:         t.ObjectId,
						ProvenanceId: t.ProvenanceId,
					},
				},
			}
		}
		result = append(result, item)
	}
	return result, nil
}

// Triples implements mixer.Triples handler.
func Triples(
	ctx context.Context,
	in *pbv1.TriplesRequest,
	store *store.Store,
	metadata *resource.Metadata,
) (*pbv1.TriplesResponse, error) {
	node := in.GetNode()
	direction := in.GetDirection()
	token := in.GetNextToken()
	if direction != util.DirectionOut && direction != util.DirectionIn {
		return nil, status.Errorf(
			codes.InvalidArgument, "uri should be /v1/triples/out/ or /v1/triples/in/")
	}
	if err := util.CheckValidDCIDs([]string{node}); err != nil {
		return nil, err
	}
	if direction == util.DirectionOut && strings.HasPrefix(node, "dc/o/") {
		resp, err := getObsTriples(ctx, store, metadata, []string{node})
		if err != nil {
			return nil, err
		}
		if len(resp) == 0 {
			return &pbv1.TriplesResponse{}, nil
		}
		return &pbv1.TriplesResponse{
			Triples: resp[0].Triples,
		}, nil
	}

	propsResp, err := properties.Properties(
		ctx, &pbv1.PropertiesRequest{
			Node:      node,
			Direction: direction,
		},
		store,
	)
	if err != nil {
		return nil, err
	}
	properties := propsResp.GetProperties()
	data, pi, err := propertyvalues.Fetch(
		ctx,
		store,
		[]string{node},
		properties,
		0,
		token,
		direction,
	)
	if err != nil {
		return nil, err
	}
	res := &pbv1.TriplesResponse{
		Triples: map[string]*pbv1.NodeInfoCollection{},
	}
	for p := range data[node] {
		res.Triples[p] = &pbv1.NodeInfoCollection{
			Nodes: propertyvalues.MergeTypedNodes(data[node][p]),
		}
	}
	if pi != nil {
		nextToken, err := util.EncodeProto(pi)
		if err != nil {
			return nil, err
		}
		res.NextToken = nextToken
	}
	return res, nil
}

// BulkTriples implements mixer.BulkTriples handler.
func BulkTriples(
	ctx context.Context,
	in *pbv1.BulkTriplesRequest,
	store *store.Store,
	metadata *resource.Metadata,
) (*pbv1.BulkTriplesResponse, error) {
	dcids := in.GetNodes()
	direction := in.GetDirection()
	token := in.GetNextToken()
	if direction != util.DirectionOut && direction != util.DirectionIn {
		return nil, status.Errorf(
			codes.InvalidArgument, "uri should be /v1/triples/out/ or /v1/triples/in/")
	}
	if err := util.CheckValidDCIDs(dcids); err != nil {
		return nil, err
	}
	// Need to fetch additional information for observation node.
	var regularDcids, obsDcids []string
	for _, dcid := range dcids {
		if strings.HasPrefix(dcid, "dc/o/") {
			obsDcids = append(obsDcids, dcid)
		} else {
			regularDcids = append(regularDcids, dcid)
		}
	}

	bulkPropsResp, err := properties.BulkProperties(
		ctx, &pbv1.BulkPropertiesRequest{
			Nodes:     regularDcids,
			Direction: direction,
		},
		store,
	)
	if err != nil {
		return nil, err
	}
	bulkProps := bulkPropsResp.GetData()
	entityProps := map[string]map[string]struct{}{}
	for _, e := range regularDcids {
		entityProps[e] = map[string]struct{}{}
	}
	properties := []string{}
	for _, resp := range bulkProps {
		for _, p := range resp.GetProperties() {
			entityProps[resp.GetNode()][p] = struct{}{}
		}
		properties = util.MergeDedupe(properties, resp.GetProperties())
	}
	data, pi, err := propertyvalues.Fetch(
		ctx,
		store,
		regularDcids,
		properties,
		0,
		token,
		direction,
	)
	if err != nil {
		return nil, err
	}
	res := &pbv1.BulkTriplesResponse{
		Data: []*pbv1.BulkTriplesResponse_NodeTriples{},
	}
	triplesByEntity := map[string]map[string][]*pb.EntityInfo{}
	for _, n := range regularDcids {
		triplesByEntity[n] = map[string][]*pb.EntityInfo{}
	}
	for n := range data {
		for p := range data[n] {
			if _, ok := entityProps[n][p]; ok {
				nodes := propertyvalues.MergeTypedNodes(data[n][p])
				if len(nodes) > 0 {
					triplesByEntity[n][p] = nodes
				}
			}
		}
	}
	for _, n := range regularDcids {
		entityTriples := &pbv1.BulkTriplesResponse_NodeTriples{
			Node:    n,
			Triples: map[string]*pbv1.NodeInfoCollection{},
		}
		for p := range triplesByEntity[n] {
			entityTriples.Triples[p] = &pbv1.NodeInfoCollection{
				Nodes: triplesByEntity[n][p],
			}
		}
		res.Data = append(res.Data, entityTriples)
	}

	if pi != nil {
		nextToken, err := util.EncodeProto(pi)
		if err != nil {
			return nil, err
		}
		res.NextToken = nextToken
	}

	if direction == util.DirectionOut && len(obsDcids) > 0 {
		obsResp, err := getObsTriples(ctx, store, metadata, obsDcids)
		if err != nil {
			return nil, err
		}
		res.Data = append(res.Data, obsResp...)
	}
	return res, nil
}
