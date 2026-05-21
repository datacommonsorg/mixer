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

package server

import (
	"context"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/topic"
	"github.com/datacommonsorg/mixer/internal/server/v2/resolve"
)

// topicExpanderAdapter adapts *topic.TopicCacheManager to the resolve.TopicExpander interface.
type topicExpanderAdapter struct {
	m *topic.TopicCacheManager
}

// newTopicExpander is a Server factory method to instantiate and return a resolve.TopicExpander.
// It hides concrete adapter details entirely from core handlers.
func (s *Server) newTopicExpander() resolve.TopicExpander {
	if s.topicCacheManager == nil {
		return nil
	}
	return &topicExpanderAdapter{m: s.topicCacheManager}
}

// ExpandRoots resolves root topics using the underlying TopicCacheManager.
func (a *topicExpanderAdapter) ExpandRoots(ctx context.Context, expandTopics bool) ([]*pbv2.ResolveResponse_Entity_Candidate, error) {
	if a.m == nil {
		return nil, nil
	}
	return a.m.ExpandRoots(ctx, expandTopics)
}

// ExpandTopic resolves children for a given topic DCID.
func (a *topicExpanderAdapter) ExpandTopic(ctx context.Context, topicDcid string, expandTopics bool) ([]*pbv2.ResolveResponse_Entity_Candidate, error) {
	if a.m == nil {
		return nil, nil
	}
	return a.m.ExpandTopic(ctx, topicDcid, expandTopics)
}

// GetTopicDisplayName retrieves display names for topics.
func (a *topicExpanderAdapter) GetTopicDisplayName(ctx context.Context, topicDcid string) string {
	if a.m == nil {
		return ""
	}
	return a.m.GetTopicDisplayName(ctx, topicDcid)
}

// GetSVPropertyInfos maps dynamic Statistical Variable metadata into resolve package stubs.
func (a *topicExpanderAdapter) GetSVPropertyInfos(ctx context.Context, svDcids []string) (map[string]resolve.SVPropertyInfo, error) {
	if a.m == nil {
		return nil, nil
	}
	infos, err := a.m.GetStatVarInfos(ctx, svDcids)
	if err != nil {
		return nil, err
	}
	res := make(map[string]resolve.SVPropertyInfo, len(infos))
	for k, info := range infos {
		if info != nil {
			res[k] = resolve.SVPropertyInfo{
				Name:                  info.Name,
				ObservationProperties: info.ObservationProperties,
			}
		}
	}
	return res, nil
}
