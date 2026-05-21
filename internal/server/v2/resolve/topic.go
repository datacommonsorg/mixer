// Copyright 2026 Google LLC
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

package resolve

import (
	"context"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ResolveTopics resolves explicit topic DCIDs or root topics using the provided TopicExpander.
func ResolveTopics(
	ctx context.Context,
	topicExpander TopicExpander,
	nodes []string,
	expandTopics bool,
) (*pbv2.ResolveResponse, error) {
	if topicExpander == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "Topic resolution is not available in this environment.")
	}

	if len(nodes) == 0 || (len(nodes) == 1 && nodes[0] == "") {
		return resolveRootTopics(ctx, topicExpander, nodes, expandTopics)
	}

	return resolveSpecifiedTopics(ctx, topicExpander, nodes, expandTopics)
}

// resolveRootTopics processes root topic expansion when query nodes are omitted or empty.
func resolveRootTopics(
	ctx context.Context,
	topicExpander TopicExpander,
	nodes []string,
	expandTopics bool,
) (*pbv2.ResolveResponse, error) {
	roots, err := topicExpander.ExpandRoots(ctx, expandTopics)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to resolve root topics: %v", err)
	}
	nodeStr := ""
	if len(nodes) == 1 {
		nodeStr = nodes[0]
	}
	return &pbv2.ResolveResponse{
		Entities: []*pbv2.ResolveResponse_Entity{
			{
				Node:       nodeStr,
				Candidates: roots,
			},
		},
	}, nil
}

// resolveSpecifiedTopics processes hierarchy expansion for explicit topic DCIDs.
func resolveSpecifiedTopics(
	ctx context.Context,
	topicExpander TopicExpander,
	nodes []string,
	expandTopics bool,
) (*pbv2.ResolveResponse, error) {
	resolveResponse := &pbv2.ResolveResponse{
		Entities: make([]*pbv2.ResolveResponse_Entity, 0, len(nodes)),
	}

	for _, node := range nodes {
		entity := &pbv2.ResolveResponse_Entity{
			Node: node,
		}

		candidates, err := topicExpander.ExpandTopic(ctx, node, expandTopics)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to expand topic %s: %v", node, err)
		}

		if len(candidates) > 0 {
			topCand := &pbv2.ResolveResponse_Entity_Candidate{
				Dcid:     node,
				TypeOf:   []string{TopicDominantType},
				Name:     topicExpander.GetTopicDisplayName(ctx, node),
				Children: candidates,
			}
			entity.Candidates = []*pbv2.ResolveResponse_Entity_Candidate{topCand}
		}

		resolveResponse.Entities = append(resolveResponse.Entities, entity)
	}

	return resolveResponse, nil
}
