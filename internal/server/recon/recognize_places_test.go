// Copyright 2023 Google LLC
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

// Package recon contains code for recon.
package recon

import (
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestTokenize(t *testing.T) {
	for _, c := range []struct {
		query string
		want  []string
	}{
		{
			"alpha,beta, gamma delta  ",
			[]string{"alpha", ",", "beta", ",", "gamma", "delta"},
		},
		{
			" alpha  ,beta,gamma,  delta eta",
			[]string{"alpha", ",", "beta", ",", "gamma", ",", "delta", "eta"},
		},
	} {
		got := tokenize(c.query)
		if diff := cmp.Diff(got, c.want); diff != "" {
			t.Errorf("Tokenize query %s got diff: %s", c.query, diff)
		}
	}
}

func TestFindPlaceCandidates(t *testing.T) {
	recogPlaceMap := map[string]*pb.RecogPlaces{
		"los": {
			Places: []*pb.RecogPlace{
				{
					Names: []*pb.RecogPlace_Name{
						{Parts: []string{"los", "angeles"}},
					},
					Dcid: "geoId/Los_Angeles",
				},
				{
					Names: []*pb.RecogPlace_Name{
						{Parts: []string{"los", "altos"}},
						{Parts: []string{"los", "altos", "city"}},
					},
					Dcid: "geoId/Los_Altos",
				},
				{
					Names: []*pb.RecogPlace_Name{
						{Parts: []string{"los", "altos", "hills"}},
					},
					Dcid: "geoId/Los_Altos_Hills",
				},
			},
		},
	}

	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, c := range []struct {
		tokens         []string
		wantNumTokens  int
		wantCandidates *pb.RecogPlaces
	}{
		{
			[]string{"Mountain", "View", "OMG"},
			0,
			nil,
		},
		{
			[]string{"Los", "Altos", "is", "a", "city", ",", "wow", "!"},
			2,
			&pb.RecogPlaces{
				Places: []*pb.RecogPlace{
					{
						Names: []*pb.RecogPlace_Name{
							{Parts: []string{"los", "altos"}},
							{Parts: []string{"los", "altos", "city"}},
						},
						Dcid: "geoId/Los_Altos",
					},
				},
			},
		},
	} {
		numTokens, candidates := findPlaceCandidates(c.tokens, recogPlaceMap)
		if numTokens != c.wantNumTokens {
			t.Errorf("findPlaceCandidates(%v) numTokens = %d, want %d",
				c.tokens, numTokens, c.wantNumTokens)
		}
		if numTokens == 0 {
			continue
		}
		if diff := cmp.Diff(candidates, c.wantCandidates, cmpOpts); diff != "" {
			t.Errorf("findPlaceCandidates(%v) got diff: %s", c.tokens, diff)
		}
	}
}
