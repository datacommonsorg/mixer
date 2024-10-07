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
						},
					},
					{Tokens: []string{"!?"}},
				},
			},
			&pb.TokenSpans{
				Spans: []*pb.TokenSpans_Span{
					{Tokens: []string{"Really?"}},
					{
						Tokens: []string{"Mountain", "View", ",", "California", ",", "USA"},
						Places: []*pb.RecogPlace{
							{
								Dcid:             "geoId/Moutain_View",
								ContainingPlaces: []string{"country/USA", "geoId/CA"},
							},
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
	pr := newPlaceRecognition()

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
							{Dcid: "geoId/MTV2", Population: 2102},
							{Dcid: "geoId/MTV3", Population: 9103},
							{Dcid: "geoId/MTV1", Population: 1101},
							{Dcid: "geoId/MTV4", Population: 9104},
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
							{Dcid: "geoId/MTV4", Population: 9104},
							{Dcid: "geoId/MTV3", Population: 9103},
							{Dcid: "geoId/MTV2", Population: 2102},
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
							{Dcid: "geoId/MTV1", Population: 9101},
							{Dcid: "geoId/MTV2", Population: 9102},
						},
					},
				},
			},
			&pb.TokenSpans{
				Spans: []*pb.TokenSpans_Span{
					{
						Tokens: []string{"Mountain", "View"},
						Places: []*pb.RecogPlace{
							{Dcid: "geoId/MTV2", Population: 9102},
							{Dcid: "geoId/MTV1", Population: 9101},
						},
					},
				},
			},
		},
	} {
		got := pr.rankAndTrimCandidates(c.tokenSpans, 2)
		if diff := cmp.Diff(got, c.want, cmpOpts); diff != "" {
			t.Errorf("rankAndTrimCandidates(%v) got diff: %s", c.tokenSpans, diff)
		}
	}
}

func TestGetId2Span(t *testing.T) {
	for _, c := range []struct {
		query string
		want  map[string]map[string]struct{}
	}{
		{
			"entities1, Entities^2 , and entities,^2",
			map[string]map[string]struct{}{
				// strip a token of ","
				"entities1": {
					"entities1,": struct{}{},
				},
				// combine 2 tokens and strip them of "," and "^", and convert them to
				// all lowercase
				"entities1 entities2": {
					"entities1, Entities^2":   struct{}{},
					"entities1, Entities^2 ,": struct{}{},
				},
				// combine 3 tokens
				"entities1 entities2 and": {
					"entities1, Entities^2 , and": struct{}{},
				},
				// combine 4 tokens
				"entities1 entities2 and entities2": {
					"entities1, Entities^2 , and entities,^2": struct{}{},
				},
				// 3 tokens that map to the same id
				"entities2": {
					"Entities^2":   struct{}{},
					"Entities^2 ,": struct{}{},
					"entities,^2":  struct{}{},
				},
				// following are the rest of the ids generated from the query that have
				// some combination of the processing done in previous cases
				"entities2 and": {
					"Entities^2 , and": struct{}{},
				},
				"entities2 and entities2": {
					"Entities^2 , and entities,^2": struct{}{},
				},
				"and": {
					"and":   struct{}{},
					", and": struct{}{},
				},
				"and entities2": {
					"and entities,^2":   struct{}{},
					", and entities,^2": struct{}{},
				},
			},
		},
	} {
		got := getId2Span(c.query)
		if diff := cmp.Diff(got, c.want); diff != "" {
			t.Errorf("GetId2Span for query %s got diff: %s", c.query, diff)
		}
	}
}

func TestSplitQueryBySpan(t *testing.T) {
	for _, c := range []struct {
		query string
		span  string
		want  []string
	}{
		{
			"ab cd ef g",
			"cd",
			[]string{"ab", "cd", "ef g"},
		},
		// termination characters "," and ";" should be valid
		{
			"ab,cd;ef,g",
			"cd",
			[]string{"ab,", "cd", ";ef,g"},
		},
		// termination characters " " and "." should be valid
		{
			"ab cd.ef,g",
			"cd",
			[]string{"ab", "cd", ".ef,g"},
		},
		// span found at the start of the query
		{
			"ab cd ef g",
			"ab",
			[]string{"ab", "cd ef g"},
		},
		// span found at the end of the query
		{
			"ab cd ef g",
			"ef g",
			[]string{"ab cd", "ef g"},
		},
		// span found multiple times in the query
		{
			"ab cd ef ab g",
			"ab",
			[]string{"ab", "cd ef", "ab", "g"},
		},
		// all the words in the query are the span
		{
			"ab;ab,ab ab",
			"ab",
			[]string{"ab", ";", "ab", ",", "ab", "ab"},
		},
		// span found multiple times in the query but only the last case is valid
		// because first case is part of another word
		{
			"abcd ef ab g",
			"ab",
			[]string{"abcd ef", "ab", "g"},
		},
		// span found multiple times in the query but none of the cases are valid
		// because all cases are part of another word
		{
			"abcd efab g",
			"ab",
			[]string{"abcd efab g"},
		},
		// single word span found over two words in the query is not valid
		{
			"ab cd ef g",
			"efg",
			[]string{"ab cd ef g"},
		},
		// two word span found in the query but one of the words is not a complete
		// word in the query, so not valid
		{
			"ab cd ef g",
			"cd e",
			[]string{"ab cd ef g"},
		},
		// span is not found in the query
		{
			"ab cd ef g",
			"hi",
			[]string{"ab cd ef g"},
		},
	} {
		got := splitQueryBySpan(c.query, c.span)
		if diff := cmp.Diff(got, c.want); diff != "" {
			t.Errorf("SplitQueryBySpan for query %s and span %s got diff: %s", c.query, c.span, diff)
		}
	}
}

func TestGetItemsForSpans(t *testing.T) {
	// this transforms protobuf messages to be used in cmp.Diff
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}
	span2Item := map[string]*pb.RecognizeEntitiesResponse_Item{
		"a^b":    {Span: "a^b", Entities: []*pb.RecognizeEntitiesResponse_Entity{{Dcid: "ab"}}},
		"cd, ef": {Span: "cd, ef", Entities: []*pb.RecognizeEntitiesResponse_Entity{{Dcid: "cdef"}}},
	}
	for _, c := range []struct {
		query string
		want  []*pb.RecognizeEntitiesResponse_Item
	}{
		// no spans recognized
		{
			"ab cd ef g",
			[]*pb.RecognizeEntitiesResponse_Item{{Span: "ab cd ef g"}},
		},
		// one span recognized in two spots
		{
			"a^b cd ef a^b",
			[]*pb.RecognizeEntitiesResponse_Item{{Span: "a^b", Entities: []*pb.RecognizeEntitiesResponse_Entity{{Dcid: "ab"}}}, {Span: "cd ef"}, {Span: "a^b", Entities: []*pb.RecognizeEntitiesResponse_Entity{{Dcid: "ab"}}}},
		},
		// two different spans recognized in the query
		{
			"a^b cd, ef g",
			[]*pb.RecognizeEntitiesResponse_Item{{Span: "a^b", Entities: []*pb.RecognizeEntitiesResponse_Entity{{Dcid: "ab"}}}, {Span: "cd, ef", Entities: []*pb.RecognizeEntitiesResponse_Entity{{Dcid: "cdef"}}}, {Span: "g"}},
		},
	} {
		got := getItemsForSpans([]string{"cd, ef", "a^b"}, c.query, span2Item)
		if diff := cmp.Diff(got, c.want, cmpOpts); diff != "" {
			t.Errorf("GetItemsForSpans for query %s got diff: %s", c.query, diff)
		}
	}
}
