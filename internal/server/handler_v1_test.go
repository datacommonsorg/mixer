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

package server

import (
	"context"
	"net/http"
	"testing"

	"github.com/datacommonsorg/mixer/internal/featureflags"
	"github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestBulkVariableInfo(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s := Server{
		store:      &store.Store{},
		metadata:   &resource.Metadata{},
		flags:      &featureflags.Flags{},
		httpClient: &http.Client{},
	}

	for _, tc := range []struct {
		desc           string
		remoteMixer    string
		statvars       []string
		localResponse  *pbv1.BulkVariableInfoResponse
		remoteResponse *pbv1.BulkVariableInfoResponse
		want           *pbv1.BulkVariableInfoResponse
	}{{
		desc:        "local only",
		remoteMixer: "",
		statvars:    []string{"v1", "v2"},
		localResponse: &pbv1.BulkVariableInfoResponse{
			Data: []*pbv1.VariableInfoResponse{
				{
					Node: "v1",
					Info: &proto.StatVarSummary{
						ProvenanceSummary: map[string]*proto.StatVarSummary_ProvenanceSummary{
							"T1": {ObservationCount: 1},
							"T2": {ObservationCount: 2},
						},
					},
				},
				{
					Node: "v2",
					Info: &proto.StatVarSummary{
						ProvenanceSummary: map[string]*proto.StatVarSummary_ProvenanceSummary{
							"T3": {ObservationCount: 3},
							"T4": {ObservationCount: 4},
						},
					},
				},
			},
		},
		want: &pbv1.BulkVariableInfoResponse{
			Data: []*pbv1.VariableInfoResponse{
				{
					Node: "v1",
					Info: &proto.StatVarSummary{
						ProvenanceSummary: map[string]*proto.StatVarSummary_ProvenanceSummary{
							"T1": {ObservationCount: 1},
							"T2": {ObservationCount: 2},
						},
					},
				},
				{
					Node: "v2",
					Info: &proto.StatVarSummary{
						ProvenanceSummary: map[string]*proto.StatVarSummary_ProvenanceSummary{
							"T3": {ObservationCount: 3},
							"T4": {ObservationCount: 4},
						},
					},
				},
			},
		},
	}, {
		desc:        "remote only",
		remoteMixer: "http://foo/bar",
		statvars:    []string{"v2", "v3"},
		remoteResponse: &pbv1.BulkVariableInfoResponse{
			Data: []*pbv1.VariableInfoResponse{
				{
					Node: "v2",
					Info: &proto.StatVarSummary{
						ProvenanceSummary: map[string]*proto.StatVarSummary_ProvenanceSummary{
							"TR1": {ObservationCount: 11},
							"TR2": {ObservationCount: 12},
						},
					},
				},
				{
					Node: "v3",
					Info: &proto.StatVarSummary{
						ProvenanceSummary: map[string]*proto.StatVarSummary_ProvenanceSummary{
							"TR3": {ObservationCount: 13},
							"TR4": {ObservationCount: 14},
						},
					},
				},
			},
		},
		want: &pbv1.BulkVariableInfoResponse{
			Data: []*pbv1.VariableInfoResponse{
				{
					Node: "v2",
					Info: &proto.StatVarSummary{
						ProvenanceSummary: map[string]*proto.StatVarSummary_ProvenanceSummary{
							"TR1": {ObservationCount: 11},
							"TR2": {ObservationCount: 12},
						},
					},
				},
				{
					Node: "v3",
					Info: &proto.StatVarSummary{
						ProvenanceSummary: map[string]*proto.StatVarSummary_ProvenanceSummary{
							"TR3": {ObservationCount: 13},
							"TR4": {ObservationCount: 14},
						},
					},
				},
			},
		},
	}, {
		desc:        "combined",
		remoteMixer: "http://foo/bar",
		statvars:    []string{"v1", "v2", "v3"},
		localResponse: &pbv1.BulkVariableInfoResponse{
			Data: []*pbv1.VariableInfoResponse{
				{
					Node: "v1",
					Info: &proto.StatVarSummary{
						ProvenanceSummary: map[string]*proto.StatVarSummary_ProvenanceSummary{
							"T1": {ObservationCount: 1},
							"T2": {ObservationCount: 2},
						},
					},
				},
				{
					Node: "v2",
					Info: &proto.StatVarSummary{
						ProvenanceSummary: map[string]*proto.StatVarSummary_ProvenanceSummary{
							"T3": {ObservationCount: 3},
							"T4": {ObservationCount: 4},
						},
					},
				},
			},
		},
		remoteResponse: &pbv1.BulkVariableInfoResponse{
			Data: []*pbv1.VariableInfoResponse{
				{
					Node: "v2",
					Info: &proto.StatVarSummary{
						ProvenanceSummary: map[string]*proto.StatVarSummary_ProvenanceSummary{
							"TR1": {ObservationCount: 11},
							"TR2": {ObservationCount: 12},
						},
					},
				},
				{
					Node: "v3",
					Info: &proto.StatVarSummary{
						ProvenanceSummary: map[string]*proto.StatVarSummary_ProvenanceSummary{
							"TR3": {ObservationCount: 13},
							"TR4": {ObservationCount: 14},
						},
					},
				},
			},
		},
		want: &pbv1.BulkVariableInfoResponse{
			Data: []*pbv1.VariableInfoResponse{
				{
					Node: "v1",
					Info: &proto.StatVarSummary{
						ProvenanceSummary: map[string]*proto.StatVarSummary_ProvenanceSummary{
							"T1": {ObservationCount: 1},
							"T2": {ObservationCount: 2},
						},
					},
				},
				{
					Node: "v2",
					Info: &proto.StatVarSummary{
						ProvenanceSummary: map[string]*proto.StatVarSummary_ProvenanceSummary{
							"T3": {ObservationCount: 3},
							"T4": {ObservationCount: 4},
						},
					},
				},
				{
					Node: "v3",
					Info: &proto.StatVarSummary{
						ProvenanceSummary: map[string]*proto.StatVarSummary_ProvenanceSummary{
							"TR3": {ObservationCount: 13},
							"TR4": {ObservationCount: 14},
						},
					},
				},
			},
		},
	}} {
		localBulkVariableInfoFunc = func(_ context.Context, _ *pbv1.BulkVariableInfoRequest, _ *store.Store) (*pbv1.BulkVariableInfoResponse, error) {
			return tc.localResponse, nil
		}
		remoteBulkVariableInfoFunc = func(_ *Server, _ *pbv1.BulkVariableInfoRequest, remoteAPIPath string) (*pbv1.BulkVariableInfoResponse, error) {
			expectedPath := "/v2/bulk/info/variable"
			if remoteAPIPath != expectedPath {
				t.Errorf("%s: expected remoteAPIPath to be %s, got %s", tc.desc, expectedPath, remoteAPIPath)
			}
			return tc.remoteResponse, nil
		}
		s.metadata.RemoteMixerDomain = tc.remoteMixer
		request := pbv1.BulkVariableInfoRequest{
			Nodes: tc.statvars,
		}
		got, _ := s.BulkVariableInfo(ctx, &request)
		if diff := cmp.Diff(got, tc.want, protocmp.Transform()); diff != "" {
			t.Errorf("%s: Unexpected diff (-want +got):\n%s", tc.desc, diff)
		}
	}
}
