// Copyright 2020 Google LLC
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
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestNoBigTable(t *testing.T) {
	ctx := context.Background()
	s := NewServer(nil, nil, nil, nil)
	resp, err := s.GetLandingPageData(ctx, &pb.GetLandingPageDataRequest{
		Place: "geoId/06",
	})
	if err != nil {
		t.Errorf("Response got error: %s", err)
	}
	expected := &pb.GetLandingPageDataResponse{}
	if diff := cmp.Diff(&resp, &expected, protocmp.Transform()); diff != "" {
		t.Errorf("payload got diff: %v", diff)
	}
}
