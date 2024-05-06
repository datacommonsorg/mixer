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
	"database/sql"
	"net/http"
	"testing"

	"github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/go-test/deep"
)

func TestBulkVariableInfo(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s := Server{
		store: &store.Store{
			SQLClient: &sql.DB{},
		},
		metadata:   &resource.Metadata{},
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
						PlaceTypeSummary: map[string]*proto.StatVarSummary_PlaceTypeSummary{
							"T1": {PlaceCount: 1},
							"T2": {PlaceCount: 2},
						},
					},
				},
				{
					Node: "v2",
					Info: &proto.StatVarSummary{
						PlaceTypeSummary: map[string]*proto.StatVarSummary_PlaceTypeSummary{
							"T3": {PlaceCount: 3},
							"T4": {PlaceCount: 4},
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
						PlaceTypeSummary: map[string]*proto.StatVarSummary_PlaceTypeSummary{
							"T1": {PlaceCount: 1},
							"T2": {PlaceCount: 2},
						},
					},
				},
				{
					Node: "v2",
					Info: &proto.StatVarSummary{
						PlaceTypeSummary: map[string]*proto.StatVarSummary_PlaceTypeSummary{
							"T3": {PlaceCount: 3},
							"T4": {PlaceCount: 4},
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
						PlaceTypeSummary: map[string]*proto.StatVarSummary_PlaceTypeSummary{
							"TR1": {PlaceCount: 11},
							"TR2": {PlaceCount: 12},
						},
					},
				},
				{
					Node: "v3",
					Info: &proto.StatVarSummary{
						PlaceTypeSummary: map[string]*proto.StatVarSummary_PlaceTypeSummary{
							"TR3": {PlaceCount: 13},
							"TR4": {PlaceCount: 14},
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
						PlaceTypeSummary: map[string]*proto.StatVarSummary_PlaceTypeSummary{
							"TR1": {PlaceCount: 11},
							"TR2": {PlaceCount: 12},
						},
					},
				},
				{
					Node: "v3",
					Info: &proto.StatVarSummary{
						PlaceTypeSummary: map[string]*proto.StatVarSummary_PlaceTypeSummary{
							"TR3": {PlaceCount: 13},
							"TR4": {PlaceCount: 14},
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
						PlaceTypeSummary: map[string]*proto.StatVarSummary_PlaceTypeSummary{
							"T1": {PlaceCount: 1},
							"T2": {PlaceCount: 2},
						},
					},
				},
				{
					Node: "v2",
					Info: &proto.StatVarSummary{
						PlaceTypeSummary: map[string]*proto.StatVarSummary_PlaceTypeSummary{
							"T3": {PlaceCount: 3},
							"T4": {PlaceCount: 4},
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
						PlaceTypeSummary: map[string]*proto.StatVarSummary_PlaceTypeSummary{
							"TR1": {PlaceCount: 11},
							"TR2": {PlaceCount: 12},
						},
					},
				},
				{
					Node: "v3",
					Info: &proto.StatVarSummary{
						PlaceTypeSummary: map[string]*proto.StatVarSummary_PlaceTypeSummary{
							"TR3": {PlaceCount: 13},
							"TR4": {PlaceCount: 14},
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
						PlaceTypeSummary: map[string]*proto.StatVarSummary_PlaceTypeSummary{
							"T1": {PlaceCount: 1},
							"T2": {PlaceCount: 2},
						},
					},
				},
				{
					Node: "v2",
					Info: &proto.StatVarSummary{
						PlaceTypeSummary: map[string]*proto.StatVarSummary_PlaceTypeSummary{
							"T3": {PlaceCount: 3},
							"T4": {PlaceCount: 4},
						},
					},
				},
				{
					Node: "v3",
					Info: &proto.StatVarSummary{
						PlaceTypeSummary: map[string]*proto.StatVarSummary_PlaceTypeSummary{
							"TR3": {PlaceCount: 13},
							"TR4": {PlaceCount: 14},
						},
					},
				},
			},
		},
	}} {
		localBulkVariableInfoFunc = func(_ context.Context, _ *pbv1.BulkVariableInfoRequest, _ *store.Store) (*pbv1.BulkVariableInfoResponse, error) {
			return tc.localResponse, nil
		}
		remoteBulkVariableInfoFunc = func(_ *Server, _ *pbv1.BulkVariableInfoRequest) (*pbv1.BulkVariableInfoResponse, error) {
			return tc.remoteResponse, nil
		}
		s.metadata.RemoteMixerDomain = tc.remoteMixer
		request := pbv1.BulkVariableInfoRequest{
			Nodes: tc.statvars,
		}
		got, _ := s.BulkVariableInfo(ctx, &request)
		if diff := deep.Equal(got, tc.want); diff != nil {
			t.Errorf("%s: Unexpected diff: %v", tc.desc, diff)
		}
	}
}
