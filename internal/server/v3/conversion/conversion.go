// Copyright 2024 Google LLC
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

package conversion

import (
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	pbv3 "github.com/datacommonsorg/mixer/internal/proto/v3"
)

func V2NodeRequest(in *pbv3.NodeRequest) *pbv2.NodeRequest {
	return &pbv2.NodeRequest{
		Nodes:     in.Nodes,
		Property:  in.Property,
		Limit:     in.Limit,
		NextToken: in.NextToken,
	}
}

func V3NodeResponse(in *pbv2.NodeResponse) *pbv3.NodeResponse {
	return &pbv3.NodeResponse{
		Data:      in.Data,
		NextToken: in.NextToken,
	}
}

func V2ObservationRequest(in *pbv3.ObservationRequest) *pbv2.ObservationRequest {
	return &pbv2.ObservationRequest{
		Variable: in.Variable,
		Entity:   in.Entity,
		Date:     in.Date,
		Value:    in.Value,
		Filter:   in.Filter,
		Select:   in.Select,
	}
}

func V3ObservationResponse(in *pbv2.ObservationResponse) *pbv3.ObservationResponse {
	return &pbv3.ObservationResponse{
		ByVariable: in.ByVariable,
		Facets:     in.Facets,
	}
}

func V2ResolveRequest(in *pbv3.ResolveRequest) *pbv2.ResolveRequest {
	return &pbv2.ResolveRequest{
		Nodes:    in.Nodes,
		Property: in.Property,
	}
}

func V3ResolveResponse(in *pbv2.ResolveResponse) *pbv3.ResolveResponse {
	out := &pbv3.ResolveResponse{}
	for _, inEntity := range in.Entities {
		outEntity := &pbv3.ResolveResponse_Entity{
			Node: inEntity.Node,
		}
		for _, inCandidate := range inEntity.Candidates {
			outCandidate := &pbv3.ResolveResponse_Entity_Candidate{
				Dcid:         inCandidate.Dcid,
				DominantType: inCandidate.DominantType,
			}
			outEntity.Candidates = append(outEntity.Candidates, outCandidate)
		}
		out.Entities = append(out.Entities, outEntity)
	}
	return out
}
