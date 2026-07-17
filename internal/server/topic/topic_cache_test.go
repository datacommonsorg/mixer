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

package topic

import (
	"context"
	"sync"
	"testing"


	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

type mockNodeFetcher struct {
	mu        sync.Mutex
	callCount int
	resps     map[string]*pbv2.LinkedGraph
}

func (m *mockNodeFetcher) NodeFetchAll(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	m.mu.Lock()
	m.callCount++
	m.mu.Unlock()
	if m.resps != nil {
		return &pbv2.NodeResponse{Data: m.resps}, nil
	}
	return &pbv2.NodeResponse{}, nil
}

func TestTopicCacheManagerInMemory(t *testing.T) {
	ctx := context.Background()

	resps := map[string]*pbv2.LinkedGraph{
		"Topic": {
			Arcs: map[string]*pbv2.Nodes{
				"typeOf": {
					Nodes: []*pb.EntityInfo{
						{Dcid: "dc/topic/Root", Name: "Root Topic"},
					},
				},
			},
		},
		"dc/topic/Root": {
			Arcs: map[string]*pbv2.Nodes{
				"relevantVariableList": {
					Nodes: []*pb.EntityInfo{
						{Value: "Count_Person"},
					},
				},
			},
		},
	}
	fetcher := &mockNodeFetcher{resps: resps}
	manager := NewTopicCacheManager(nil)
	manager.InitFetcher(fetcher)

	// Assert initial state is empty
	if got := manager.CachedHierarchy(); got != nil {
		t.Fatalf("CachedHierarchy() should be nil initially")
	}

	// Assert GetHierarchy synchronously loads and updates the cache
	hierarchy, err := manager.GetHierarchy(ctx)
	if err != nil {
		t.Fatalf("GetHierarchy() failed: %v", err)
	}
	if hierarchy == nil {
		t.Fatalf("GetHierarchy() returned nil")
	}

	// Verify in-memory cache is updated
	cached := manager.CachedHierarchy()
	if cached == nil {
		t.Fatalf("CachedHierarchy() returned nil after load")
	}

	// Assert that loading again hits the in-memory cache directly and matches
	secondLoad, err := manager.GetHierarchy(ctx)
	if err != nil {
		t.Fatalf("Second GetHierarchy() failed: %v", err)
	}
	if secondLoad != cached {
		t.Errorf("Second GetHierarchy() should return the exact same cached pointer")
	}
}



func TestGetStatVarInfos(t *testing.T) {
	ctx := context.Background()
	resps := map[string]*pbv2.LinkedGraph{
		"Count_Person": {
			Arcs: map[string]*pbv2.Nodes{
				"name": {
					Nodes: []*pb.EntityInfo{
						{Name: "Person Count"},
					},
				},
				"observationProperties": {
					Nodes: []*pb.EntityInfo{
						{Value: "statVarObservation"},
					},
				},
				"entityMapping": {
					Nodes: []*pb.EntityInfo{
						{Value: "Count_Person_Column"},
					},
				},
			},
		},
	}
	fetcher := &mockNodeFetcher{resps: resps}
	manager := NewTopicCacheManager(nil)
	manager.InitFetcher(fetcher)

	// Update cache with empty hierarchy to initialize m.cache
	manager.Update(&pb.TopicHierarchy{})

	infos, err := manager.GetStatVarInfos(ctx, []string{"Count_Person"})
	if err != nil {
		t.Fatalf("GetStatVarInfos failed: %v", err)
	}
	info, ok := infos["Count_Person"]
	if !ok || info.Name != "Person Count" || len(info.ObservationProperties) == 0 || info.ObservationProperties[0] != "statVarObservation" {
		t.Errorf("GetStatVarInfos mismatch: got %+v", info)
	}

	// Fetch again to verify cache hit
	beforeCount := fetcher.callCount
	_, _ = manager.GetStatVarInfos(ctx, []string{"Count_Person"})
	if fetcher.callCount != beforeCount {
		t.Errorf("Expected cache hit, but callCount increased from %d to %d", beforeCount, fetcher.callCount)
	}
}

func TestTopicExpansion(t *testing.T) {
	ctx := context.Background()
	resps := map[string]*pbv2.LinkedGraph{
		"Count_Person": {
			Arcs: map[string]*pbv2.Nodes{
				"name": {Nodes: []*pb.EntityInfo{{Name: "Person Count"}}},
				"observationProperties": {Nodes: []*pb.EntityInfo{{Value: "statVarObservation"}}},
			},
		},
	}
	fetcher := &mockNodeFetcher{resps: resps}
	manager := NewTopicCacheManager(nil)
	manager.InitFetcher(fetcher)

	// Set up mock hierarchy
	h := &pb.TopicHierarchy{
		RootTopicDcids: []string{"dc/topic/Root"},
		Topics: map[string]*pb.TopicNode{
			"dc/topic/Root": {
				Dcid:              "dc/topic/Root",
				Name:              "Root Topic",
				RelevantVariables: []string{"dc/topic/SubTopic"},
			},
			"dc/topic/SubTopic": {
				Dcid:              "dc/topic/SubTopic",
				Name:              "Sub Topic",
				RelevantVariables: []string{"Count_Person"},
			},
		},
	}
	manager.Update(h)

	tests := []struct {
		desc         string
		expandTopics bool
		want         []*pbv2.ResolveResponse_Entity_Candidate
	}{
		{
			desc:         "Immediate direct children (expandTopics=false)",
			expandTopics: false,
			want: []*pbv2.ResolveResponse_Entity_Candidate{
				{
					Dcid:   "dc/topic/Root",
					TypeOf: []string{"Topic"},
					Name:   "Root Topic",
					Children: []*pbv2.ResolveResponse_Entity_Candidate{
						{
							Dcid:   "dc/topic/SubTopic",
							TypeOf: []string{"Topic"},
							Name:   "Sub Topic",
						},
					},
				},
			},
		},
		{
			desc:         "Recursive leaf variable expansion (expandTopics=true)",
			expandTopics: true,
			want: []*pbv2.ResolveResponse_Entity_Candidate{
				{
					Dcid:   "dc/topic/Root",
					TypeOf: []string{"Topic"},
					Name:   "Root Topic",
					Children: []*pbv2.ResolveResponse_Entity_Candidate{
						{
							Dcid:                  "Count_Person",
							TypeOf:                []string{"StatisticalVariable"},
							Name:                  "Person Count",
							ObservationProperties: []string{"statVarObservation"},
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			got, err := manager.ExpandRoots(ctx, tc.expandTopics)
			if err != nil {
				t.Fatalf("ExpandRoots(%t) failed: %v", tc.expandTopics, err)
			}
			if diff := cmp.Diff(got, tc.want, protocmp.Transform()); diff != "" {
				t.Errorf("ExpandRoots(%t) mismatch (-got +want):\n%s", tc.expandTopics, diff)
			}
		})
	}
}
