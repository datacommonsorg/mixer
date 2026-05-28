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
	"testing"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

type mockTopicExpander struct{}

func (m *mockTopicExpander) ExpandRoots(ctx context.Context, expandTopics bool) ([]*pbv2.ResolveResponse_Entity_Candidate, error) {
	return []*pbv2.ResolveResponse_Entity_Candidate{
		{
			Dcid:   "dc/topic/Root",
			TypeOf: []string{"Topic"},
			Name:   "Root Topic",
		},
	}, nil
}

func (m *mockTopicExpander) ExpandTopic(ctx context.Context, topicDcid string, expandTopics bool) ([]*pbv2.ResolveResponse_Entity_Candidate, error) {
	if topicDcid == "dc/topic/Demo" {
		return []*pbv2.ResolveResponse_Entity_Candidate{
			{
				Dcid:                  "Count_Person",
				TypeOf:                []string{"StatisticalVariable"},
				Name:                  "Person Count",
				ObservationProperties: []string{"statVarObservation"},
			},
		}, nil
	}
	return nil, nil
}

func (m *mockTopicExpander) GetTopicDisplayName(ctx context.Context, topicDcid string) string {
	if topicDcid == "dc/topic/Demo" {
		return "Demographics Topic"
	}
	return ""
}

func (m *mockTopicExpander) GetSVPropertyInfos(ctx context.Context, svDcids []string) (map[string]SVPropertyInfo, error) {
	return map[string]SVPropertyInfo{
		"Count_Person": {
			Name:                  "Person Count",
			ObservationProperties: []string{"statVarObservation"},
		},
	}, nil
}

func TestResolveTopics(t *testing.T) {
	ctx := context.Background()
	expander := &mockTopicExpander{}

	tests := []struct {
		desc         string
		nodes        []string
		expandTopics bool
		want         *pbv2.ResolveResponse
		wantErr      bool
	}{
		{
			desc:         "Empty nodes list resolves root topics",
			nodes:        []string{},
			expandTopics: false,
			want: &pbv2.ResolveResponse{
				Entities: []*pbv2.ResolveResponse_Entity{
					{
						Node: "",
						Candidates: []*pbv2.ResolveResponse_Entity_Candidate{
							{
								Dcid:   "dc/topic/Root",
								TypeOf: []string{"Topic"},
								Name:   "Root Topic",
							},
						},
					},
				},
			},
		},
		{
			desc:         "Exact topic dcid resolution",
			nodes:        []string{"dc/topic/Demo"},
			expandTopics: true,
			want: &pbv2.ResolveResponse{
				Entities: []*pbv2.ResolveResponse_Entity{
					{
						Node: "dc/topic/Demo",
						Candidates: []*pbv2.ResolveResponse_Entity_Candidate{
							{
								Dcid:     "dc/topic/Demo",
								TypeOf:   []string{"Topic"},
								Name:     "Demographics Topic",
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
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			got, err := ResolveTopics(ctx, expander, tc.nodes, tc.expandTopics)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ResolveTopics() err: %v, wantErr: %t", err, tc.wantErr)
			}
			if !tc.wantErr {
				if diff := cmp.Diff(got, tc.want, protocmp.Transform()); diff != "" {
					t.Errorf("ResolveTopics() mismatch (-got +want):\n%s", diff)
				}
			}
		})
	}
}
