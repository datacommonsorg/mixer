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
	"github.com/datacommonsorg/mixer/internal/store/files"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func newPlaceRecognition() *placeRecognition {
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
	return &placeRecognition{
		recogPlaceStore: &files.RecogPlaceStore{
			RecogPlaceMap: recogPlaceMap,
		},
	}
}

func TestTokenize(t *testing.T) {
	for _, c := range []struct {
		query string
		want  []string
	}{
		{
			"alpha,beta, Gamma delta  ",
			[]string{"alpha", ",", "beta", ",", "Gamma", "delta"},
		},
		{
			" alpha  ,beta,Gamma,  delta eta",
			[]string{"alpha", ",", "beta", ",", "Gamma", ",", "delta", "eta"},
		},
		{
			"alpha , beta,Gamma",
			[]string{"alpha", ",", "beta", ",", "Gamma"},
		},
	} {
		got := tokenize(c.query)
		if diff := cmp.Diff(got, c.want); diff != "" {
			t.Errorf("Tokenize query %s got diff: %s", c.query, diff)
		}
	}
}

func TestFindPlaceCandidates(t *testing.T) {
	pr := newPlaceRecognition()
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
			[]string{"Los", "Altos", "hills", "is", "a", "city", ",", "wow", "!"},
			3,
			&pb.RecogPlaces{
				Places: []*pb.RecogPlace{
					{
						Names: []*pb.RecogPlace_Name{
							{Parts: []string{"los", "altos", "hills"}},
						},
						Dcid: "geoId/Los_Altos_Hills",
					},
				},
			},
		},
	} {
		numTokens, candidates := pr.findPlaceCandidates(c.tokens)
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

func TestReplaceTokensWithCandidates(t *testing.T) {
	pr := newPlaceRecognition()
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, c := range []struct {
		tokens []string
		want   *pb.TokenSpans
	}{
		{
			[]string{"Mountain", "View", "OMG"},
			&pb.TokenSpans{
				Spans: []*pb.TokenSpans_Span{
					{Tokens: []string{"Mountain"}},
					{Tokens: []string{"View"}},
					{Tokens: []string{"OMG"}},
				},
			},
		},
		{
			[]string{"Los", "Angeles", "is", "bigger", "than", "los", "Altos", "!?"},
			&pb.TokenSpans{
				Spans: []*pb.TokenSpans_Span{
					{
						Tokens: []string{"Los", "Angeles"},
						Places: []*pb.RecogPlace{
							{
								Names: []*pb.RecogPlace_Name{
									{Parts: []string{"los", "angeles"}},
								},
								Dcid: "geoId/Los_Angeles",
							},
						},
					},
					{Tokens: []string{"is"}},
					{Tokens: []string{"bigger"}},
					{Tokens: []string{"than"}},
					{
						Tokens: []string{"los", "Altos"},
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
					{Tokens: []string{"!?"}},
				},
			},
		},
	} {
		got := pr.replaceTokensWithCandidates(c.tokens)
		if diff := cmp.Diff(got, c.want, cmpOpts); diff != "" {
			t.Errorf("replaceTokensWithCandidates(%v) got diff: %s", c.tokens, diff)
		}
	}
}

func TestCombineContainedIn(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, c := range []struct {
		tokenSpans *pb.TokenSpans
		want       *pb.TokenSpans
	}{
		// NumTokens = 2.
		{
			&pb.TokenSpans{
				Spans: []*pb.TokenSpans_Span{
					{Tokens: []string{"Really?"}},
					{
						Tokens: []string{"Mountain", "View"},
						Places: []*pb.RecogPlace{
							{
								Dcid:             "geoId/Moutain_View",
								ContainingPlaces: []string{"geoId/Santa_Clara", "geoId/CA"},
							},
						},
					},
					{
						Tokens: []string{"Santa", "Clara", "County"},
						Places: []*pb.RecogPlace{
							{Dcid: "geoId/Santa_Clara"},
						},
					},
					{Tokens: []string{"!?"}},
				},
			},
			&pb.TokenSpans{
				Spans: []*pb.TokenSpans_Span{
					{Tokens: []string{"Really?"}},
					{
						Tokens: []string{"Mountain", "View", "Santa", "Clara", "County"},
						Places: []*pb.RecogPlace{
							{
								Dcid:             "geoId/Moutain_View",
								ContainingPlaces: []string{"geoId/Santa_Clara", "geoId/CA"},
							},
							{Dcid: "geoId/Santa_Clara"},
						},
					},
					{Tokens: []string{"!?"}},
				},
			},
		},
		// NumTokens = 3.
		{
			&pb.TokenSpans{
				Spans: []*pb.TokenSpans_Span{
					{Tokens: []string{"Really?"}},
					{
						Tokens: []string{"Mountain", "View"},
						Places: []*pb.RecogPlace{
							{
								Dcid:             "geoId/Moutain_View",
								ContainingPlaces: []string{"geoId/Santa_Clara", "geoId/CA"},
							},
						},
					},
					{Tokens: []string{","}},
					{
						Tokens: []string{"Santa", "Clara", "County"},
						Places: []*pb.RecogPlace{
							{Dcid: "geoId/Santa_Clara"},
							{Dcid: "wikidataId/Santa_Clara"},
						},
					},
					{Tokens: []string{"!?"}},
				},
			},
			&pb.TokenSpans{
				Spans: []*pb.TokenSpans_Span{
					{Tokens: []string{"Really?"}},
					{
						Tokens: []string{"Mountain", "View", ",", "Santa", "Clara", "County"},
						Places: []*pb.RecogPlace{
							{
								Dcid:             "geoId/Moutain_View",
								ContainingPlaces: []string{"geoId/Santa_Clara", "geoId/CA"},
							},
							{Dcid: "geoId/Santa_Clara"},
							{Dcid: "wikidataId/Santa_Clara"},
						},
					},
					{Tokens: []string{"!?"}},
				},
			},
		},
		// No combination.
		{
			&pb.TokenSpans{
				Spans: []*pb.TokenSpans_Span{
					{Tokens: []string{"Really?"}},
					{
						Tokens: []string{"Mountain", "View"},
						Places: []*pb.RecogPlace{
							{
								Dcid:             "geoId/Moutain_View",
								ContainingPlaces: []string{"geoId/Santa_Clara", "geoId/CA"},
							},
						},
					},
					{Tokens: []string{"!?"}},
				},
			},
			&pb.TokenSpans{
				Spans: []*pb.TokenSpans_Span{
					{Tokens: []string{"Really?"}},
					{
						Tokens: []string{"Mountain", "View"},
						Places: []*pb.RecogPlace{
							{
								Dcid:             "geoId/Moutain_View",
								ContainingPlaces: []string{"geoId/Santa_Clara", "geoId/CA"},
							},
						},
					},
					{Tokens: []string{"!?"}},
				},
			},
		},
	} {
		got := combineContainedIn(c.tokenSpans)
		if diff := cmp.Diff(got, c.want, cmpOpts); diff != "" {
			t.Errorf("combineContainedIn(%v) got diff: %s", c.tokenSpans, diff)
		}
	}
}

func TestRankAndTrimCandidates(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, c := range []struct {
		tokenSpans *pb.TokenSpans
		want       *pb.TokenSpans
	}{
		{
			&pb.TokenSpans{
				Spans: []*pb.TokenSpans_Span{
					{Tokens: []string{"OMG"}},
					{
						Tokens: []string{"Mountain", "View"},
						Places: []*pb.RecogPlace{
							{Dcid: "geoId/MTV2", Population: 102},
							{Dcid: "geoId/MTV3", Population: 103},
							{Dcid: "geoId/MTV1", Population: 101},
							{Dcid: "geoId/MTV4", Population: 104},
						},
					},
				},
			},
			&pb.TokenSpans{
				Spans: []*pb.TokenSpans_Span{
					{Tokens: []string{"OMG"}},
					{
						Tokens: []string{"Mountain", "View"},
						Places: []*pb.RecogPlace{
							{Dcid: "geoId/MTV4", Population: 104},
							{Dcid: "geoId/MTV3", Population: 103},
							{Dcid: "geoId/MTV2", Population: 102},
							{Dcid: "geoId/MTV1", Population: 101},
						},
					},
				},
			},
		},
		{
			&pb.TokenSpans{
				Spans: []*pb.TokenSpans_Span{
					{
						Tokens: []string{"Mountain", "View"},
						Places: []*pb.RecogPlace{
							{Dcid: "geoId/MTV1", Population: 101},
							{Dcid: "geoId/MTV2", Population: 102},
						},
					},
				},
			},
			&pb.TokenSpans{
				Spans: []*pb.TokenSpans_Span{
					{
						Tokens: []string{"Mountain", "View"},
						Places: []*pb.RecogPlace{
							{Dcid: "geoId/MTV2", Population: 102},
							{Dcid: "geoId/MTV1", Population: 101},
						},
					},
				},
			},
		},
	} {
		got := rankAndTrimCandidates(c.tokenSpans)
		if diff := cmp.Diff(got, c.want, cmpOpts); diff != "" {
			t.Errorf("rankAndTrimCandidates(%v) got diff: %s", c.tokenSpans, diff)
		}
	}
}
