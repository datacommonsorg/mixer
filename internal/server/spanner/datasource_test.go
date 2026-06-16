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

package spanner

import (
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
)

func TestProvenanceURLMapFromNodeResponse_NilGraphDoesNotPanic(t *testing.T) {
	got := provenanceURLMapFromNodeResponse(&pbv2.NodeResponse{
		Data: map[string]*pbv2.LinkedGraph{
			"dc/base/nil-graph": nil,
			"dc/base/missing-url": {
				Arcs: map[string]*pbv2.Nodes{},
			},
			"dc/base/nil-url-arc": {
				Arcs: map[string]*pbv2.Nodes{
					predUrl: nil,
				},
			},
			"dc/base/empty-url": {
				Arcs: map[string]*pbv2.Nodes{
					predUrl: {
						Nodes: []*pb.EntityInfo{
							{Value: ""},
						},
					},
				},
			},
			"dc/base/prov-1": {
				Arcs: map[string]*pbv2.Nodes{
					predUrl: {
						Nodes: []*pb.EntityInfo{
							nil,
							{Value: ""},
							{Value: "https://resolved.test/source"},
							{Value: "https://resolved.test/ignored"},
						},
					},
				},
			},
		},
	})

	want := map[string]string{
		"dc/base/prov-1": "https://resolved.test/source",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("provenanceURLMapFromNodeResponse() mismatch (-want +got):\n%s", diff)
	}
}
