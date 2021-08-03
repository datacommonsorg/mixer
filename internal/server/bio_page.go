// Copyright 2021 Google LLC
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

package server

import (
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetBioPageData implements API for Mixer.GetBioPageData.
//
// TODO(shifucun): This is only a mini version with partial data.
// Use pre-computed data from Bigtable when it's ready.
func (s *Server) GetBioPageData(
	ctx context.Context, in *pb.GetBioPageDataRequest) (
	*pb.GraphNode, error) {

	dcid := in.GetDcid()
	if dcid == "" {
		return nil, status.Errorf(
			codes.InvalidArgument, "Missing required arguments: dcid")
	}
	resp := &pb.GraphNode{
		Value: dcid,
		Neighbours: []*pb.GraphNode_LinkedNodes{
			{
				Property: "detectedProtein",
				Nodes:    []*pb.GraphNode{},
			},
		},
	}

	inNodes, err := getPropertyValuesHelper(
		ctx, s.store, []string{dcid}, "detectedProtein", false)
	if err != nil {
		return nil, err
	}
	detectedProteins := []string{}
	for _, dp := range inNodes[dcid] {
		detectedProteins = append(detectedProteins, dp.Dcid)
	}

	humanTissueNodes, err := getPropertyValuesHelper(
		ctx, s.store, detectedProteins, "humanTissue", true)
	if err != nil {
		return nil, err
	}

	scoreNodes, err := getPropertyValuesHelper(
		ctx, s.store, detectedProteins, "proteinExpressionScore", true)
	if err != nil {
		return nil, err
	}

	for _, dp := range detectedProteins {
		detectedProteinNode := &pb.GraphNode{
			Value: dp,
			Neighbours: []*pb.GraphNode_LinkedNodes{
				{
					Property: "humanTissue",
					Nodes:    []*pb.GraphNode{},
				},
				{
					Property: "proteinExpressionScore",
					Nodes:    []*pb.GraphNode{},
				},
			},
		}
		for _, tissue := range humanTissueNodes[dp] {
			detectedProteinNode.Neighbours[0].Nodes = append(
				detectedProteinNode.Neighbours[0].Nodes,
				&pb.GraphNode{
					Value: tissue.Dcid,
				},
			)
		}
		for _, score := range scoreNodes[dp] {
			detectedProteinNode.Neighbours[1].Nodes = append(
				detectedProteinNode.Neighbours[1].Nodes,
				&pb.GraphNode{
					Value: score.Dcid,
				},
			)
		}
		resp.Neighbours[0].Nodes = append(
			resp.Neighbours[0].Nodes, detectedProteinNode)
	}
	return resp, nil
}
