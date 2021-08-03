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

// GetProteinPageData implements API for Mixer.GetProteinPageData.
//
// TODO(shifucun): This is only a mini version with partial data.
// Use pre-computed data from Bigtable when it's ready.
func (s *Server) GetProteinPageData(
	ctx context.Context, in *pb.GetProteinPageDataRequest) (
	*pb.GetProteinPageDataResponse, error) {

	proteinDcid := in.GetProtein()
	if proteinDcid == "" {
		return nil, status.Errorf(
			codes.InvalidArgument, "Missing required arguments: protein")
	}
	resp := &pb.GetProteinPageDataResponse{
		Value: proteinDcid,
		Neighbours: []*pb.GetProteinPageDataResponse_LinkedNodes{
			{
				Property: "detectedProtein",
				Nodes:    []*pb.GetProteinPageDataResponse{},
			},
		},
	}

	inNodes, err := getPropertyValuesHelper(
		ctx, s.store, []string{proteinDcid}, "detectedProtein", false)
	if err != nil {
		return nil, err
	}
	detectedProteins := []string{}
	for _, dp := range inNodes[proteinDcid] {
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
		detectedProteinNode := &pb.GetProteinPageDataResponse{
			Value: dp,
			Neighbours: []*pb.GetProteinPageDataResponse_LinkedNodes{
				{
					Property: "humanTissue",
					Nodes:    []*pb.GetProteinPageDataResponse{},
				},
				{
					Property: "proteinExpressionScore",
					Nodes:    []*pb.GetProteinPageDataResponse{},
				},
			},
		}
		for _, tissue := range humanTissueNodes[dp] {
			detectedProteinNode.Neighbours[0].Nodes = append(
				detectedProteinNode.Neighbours[0].Nodes,
				&pb.GetProteinPageDataResponse{
					Value: tissue.Dcid,
				},
			)
		}
		for _, score := range scoreNodes[dp] {
			detectedProteinNode.Neighbours[1].Nodes = append(
				detectedProteinNode.Neighbours[1].Nodes,
				&pb.GetProteinPageDataResponse{
					Value: score.Dcid,
				},
			)
		}
		resp.Neighbours[0].Nodes = append(
			resp.Neighbours[0].Nodes, detectedProteinNode)
	}
	return resp, nil
}
