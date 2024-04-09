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
	"context"
	"math"
	"sort"
	"strings"
	"sync"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/files"
)

const (
	maxPlaceCandidates = 15
	// When the number of places is larger than maxPlaceCandidates, firstly pick maxPlaceCandidates
	// places, for the rest, only pick the places with
	// population >= minPopulationOverMaxPlaceCandidates.
	minPopulationOverMaxPlaceCandidates = 2000
	nameReconInProp                     = "reconName"
	nameReconOutProp                    = "dcid"
	reconNGramLimit                     = 10
)

var (
	wordSeparators = map[byte]bool{' ': true, ',': true, ';': true, '.': true}
)

// RecognizePlaces implements API for Mixer.RecognizePlaces.
func RecognizePlaces(
	ctx context.Context,
	in *pb.RecognizePlacesRequest,
	store *store.Store,
	resolveBogusName bool,
) (*pb.RecognizePlacesResponse, error) {
	pr := &placeRecognition{
		recogPlaceStore:  store.RecogPlaceStore,
		resolveBogusName: resolveBogusName,
	}

	type queryItems struct {
		query string
		items *pb.RecognizePlacesResponse_Items
	}

	var wg sync.WaitGroup
	resChan := make(chan *queryItems, len(in.GetQueries()))
	for _, query := range in.GetQueries() {
		wg.Add(1)
		go func(query string) {
			defer wg.Done()
			resChan <- &queryItems{query: query, items: pr.detectPlaces(query)}
		}(query)
	}
	wg.Wait()
	close(resChan)

	resp := &pb.RecognizePlacesResponse{
		QueryItems: map[string]*pb.RecognizePlacesResponse_Items{},
	}
	for res := range resChan {
		resp.QueryItems[res.query] = res.items
	}

	return resp, nil
}

// RecognizePlaces implements API for Mixer.RecognizePlaces.
func RecognizeEntities(
	ctx context.Context,
	in *pb.RecognizePlacesRequest,
	store *store.Store,
	resolveBogusName bool,
) (*pb.RecognizePlacesResponse, error) {

	resp := &pb.RecognizePlacesResponse{
		QueryItems: map[string]*pb.RecognizePlacesResponse_Items{},
	}
	// TODO: parallelize queries
	for _, query := range in.GetQueries() {
		id2spans := getId2Span(query)
		if id2spans == nil {
			continue
		}
		idsToResolve := []string{}
		for id := range id2spans {
			idsToResolve = append(idsToResolve, id)
		}
		resolvedIdEntities, err := GetResolvedIdEntities(ctx, store, nameReconInProp, idsToResolve, nameReconOutProp)
		if err != nil {
			continue
		}

		// go through the resolved entities and create a map of spans to the entity
		// it resolved to
		span2item := map[string]*pb.RecognizePlacesResponse_Item{}
		for _, entity := range resolvedIdEntities.GetEntities() {
			places := []*pb.RecognizePlacesResponse_Place{}
			for _, id := range entity.GetOutIds() {
				// TODO: add types to the response
				places = append(places, &pb.RecognizePlacesResponse_Place{Dcid: id})
			}
			for span := range id2spans[entity.GetInId()] {
				span2item[span] = &pb.RecognizePlacesResponse_Item{Span: span, Places: places}
			}
		}

		// get the list of resolved spans and sort them by more words first. If two
		// spans have the same number of words, sort alphabetically.
		spans := []string{}
		span2count := map[string]int{}
		for span := range span2item {
			spans = append(spans, span)
			span2count[span] = strings.Count(span, " ")
		}
		sort.Slice(spans, func(i, j int) bool {
			if span2count[spans[i]] > span2count[spans[j]] {
				return true
			} else if span2count[spans[j]] > span2count[spans[i]] {
				return false
			} else {
				return spans[i] < spans[j]
			}
		})

		// Get the response items
		queryRespItems := getItemsForSpans(spans, query, span2item)
		resp.QueryItems[query] = &pb.RecognizePlacesResponse_Items{Items: queryRespItems}
	}
	return resp, nil
}

// Takes a query and list of spans and gets the corresponding list of recognize
// places response items. With the list of spans, we want the response items to
// have a subset of non-overlapping spans. We do that with a greedy approach of
// matching the longest span in query, getting the remaining query parts, and
// recursively doing the match.
func getItemsForSpans(spans []string, query string, span2item map[string]*pb.RecognizePlacesResponse_Item) []*pb.RecognizePlacesResponse_Item {
	queryRespItems := []*pb.RecognizePlacesResponse_Item{}
	// If empty query, return empty list
	if len(query) == 0 {
		return queryRespItems
	}
	// If empty list of spans, return the query as the only item
	if len(spans) == 0 {
		return append(queryRespItems, &pb.RecognizePlacesResponse_Item{Span: query})
	}
	span := spans[0]
	queryParts := splitQueryBySpan(query, span)
	for _, part := range queryParts {
		if part == span {
			queryRespItems = append(queryRespItems, span2item[span])
		} else {
			queryRespItems = append(queryRespItems, getItemsForSpans(spans[1:], part, span2item)...)
		}
	}
	return queryRespItems
}

// Splits a query by a span into a list of parts like: non-span part, span part,
// non-span part, span part, etc. and should only split on complete words. If
// the query does not contain the span, returns a list with the query as the
// only item.
// Examples
// query: "ab cd ef ab g", span: "ab", result: ["ab", "cd ef", "ab", "g"]
// query: "ab cd ef g", span: "jk", result: ["ab cd ef g"]
func splitQueryBySpan(query string, span string) []string {
	i := 0
	parts := []string{""}

	for i < len(query) {
		// a valid span match is one that starts at the beginning of the query or
		// after a word separator
		if i == 0 || wordSeparators[query[i-1]] {
			if strings.HasPrefix(query[i:], span) {
				endIdx := i + len(span)
				// a valid span match is one that ends at the end of the query or before
				// a word separator
				if endIdx == len(query) || wordSeparators[query[endIdx]] {
					// valid span match is found so add the span to the list of parts
					parts = append(parts, span)
					// add an empty span to start the possibly next non-span part
					parts = append(parts, "")
					// move on to search the next part of the query after the span we just
					// found
					i = endIdx
					continue
				}
			}
		}
		// this current index in the query is not part of a valid span match so just
		// add the current character to the current non-span part
		parts[len(parts)-1] += string(query[i])
		// move on to search from the next index in the query
		i += 1
	}

	// trim spaces and filter out empty parts
	filteredParts := []string{}
	for _, part := range parts {
		trimmedPart := strings.TrimSpace(part)
		if len(trimmedPart) == 0 {
			continue
		}
		filteredParts = append(filteredParts, trimmedPart)
	}
	return filteredParts
}

// Gets the reconName for a span.
// The logic here should correspond to the logic for processing name into
// reconName in flume (https://source.corp.google.com/piper///depot/google3/datacommons/prophet/flume_generator/triple_helper.cc;l=168-173)
// TODO: also clean up consecutive spaces
func getReconName(span string) string {
	reconName := strings.ReplaceAll(span, " ,", "")
	reconName = strings.ReplaceAll(reconName, ",", "")
	reconName = strings.ReplaceAll(reconName, "^", "")
	reconName = strings.ToLower(reconName)
	reconName = strings.TrimSpace(reconName)
	return reconName
}

// Takes a query and returns a map of id to use for resolution to the
// original span (part of the query) for that id.
// A single id can have multiple spans because we process a span by removing
// certain characters to get an id:
// E.g., "a^b" and "ab" would both have the id "ab"
func getId2Span(query string) map[string]map[string]struct{} {
	id2spans := map[string]map[string]struct{}{}
	spanTokens := strings.Split(query, " ")
	for i := range spanTokens {
		span := ""
		maxNGramLength := int(math.Min(float64(len(spanTokens)), reconNGramLimit+1))
		// make n-grams from the span tokens
		for j := i; j < maxNGramLength; j++ {
			span = span + " " + spanTokens[j]
			span = strings.TrimSpace(span)
			id := getReconName(span)
			if len(id) < 1 {
				continue
			}
			if _, ok := id2spans[id]; !ok {
				id2spans[id] = map[string]struct{}{}
			}
			id2spans[id][span] = struct{}{}
		}
	}
	return id2spans
}

func tokenize(query string) []string {
	tokens := []string{}

	// Split by space.
	for _, partBySpace := range strings.Split(query, " ") {
		if partBySpace == "" {
			// This is due to successive spaces, or spaces as prefix or suffix.
			continue
		}

		// Check prefix.
		if string(partBySpace[0]) == "," {
			tokens = append(tokens, ",")
			partBySpace = partBySpace[1:]
			if partBySpace == "" {
				continue
			}
		}

		// Check suffix.
		hasCommaSuffix := false
		if string(partBySpace[len(partBySpace)-1]) == "," {
			hasCommaSuffix = true
			partBySpace = partBySpace[:len(partBySpace)-1]
		}

		// Split by comma. Note |partBySpace| doesn't contain any space.
		partsByComma := strings.Split(partBySpace, ",")
		nPartsByComma := len(partsByComma)
		for idx, partByComma := range partsByComma {
			tokens = append(tokens, partByComma)

			// Add the comma as a token when:
			// 1. Not the last token.
			// 2. The original |partBySpace| has comma as suffix.
			if idx != nPartsByComma-1 || hasCommaSuffix {
				tokens = append(tokens, ",")
			}
		}
	}

	return tokens
}

type placeRecognition struct {
	recogPlaceStore  *files.RecogPlaceStore
	resolveBogusName bool
}

func (p *placeRecognition) detectPlaces(
	query string) *pb.RecognizePlacesResponse_Items {
	tokenSpans := p.replaceTokensWithCandidates(tokenize(query))
	candidates := p.rankAndTrimCandidates(combineContainedIn(tokenSpans), maxPlaceCandidates)
	return formatResponse(query, candidates)
}

func (p *placeRecognition) findPlaceCandidates(
	tokens []string) (int, *pb.RecogPlaces) {
	if len(tokens) == 0 {
		return 0, nil
	}

	// Check if the first token match any abbreviated name.
	// Note: abbreviated names are case-sensitive.
	if places, ok := p.recogPlaceStore.AbbreviatedNameToPlaces[tokens[0]]; ok {
		return 1, places
	}

	key := strings.ToLower(tokens[0])
	places, ok := p.recogPlaceStore.RecogPlaceMap[key]
	if !ok {
		return 0, nil
	}

	numTokens := 1
	// We track the places matched by the span width.  Because we want to
	// always prefer to return the maximally matched span.
	candidatesByNumTokens := make(map[int]*pb.RecogPlaces)
	for _, place := range places.GetPlaces() {
		matchedNameSize := 0
		for _, name := range place.GetNames() {
			nameParts := name.GetParts()
			namePartsSize := len(nameParts)

			// To find a match, tokens cannot be shorter than name parts.
			if len(tokens) < namePartsSize {
				continue
			}

			nameMatched := true
			for i := 0; i < namePartsSize; i++ {
				if nameParts[i] != strings.ToLower(tokens[i]) {
					nameMatched = false
					break
				}
			}

			if nameMatched && matchedNameSize < namePartsSize {
				// Try to match the longest possible name.
				// For example, "New York City" should match 3 tokens instead of 2 tokens.
				matchedNameSize = namePartsSize
			}
		}
		if matchedNameSize == 0 { // This place is not matched.
			continue
		}

		// Take the max size of tokens for all matched places.
		if numTokens < matchedNameSize {
			numTokens = matchedNameSize
		}
		candidates, ok := candidatesByNumTokens[matchedNameSize]
		if !ok {
			candidatesByNumTokens[matchedNameSize] = &pb.RecogPlaces{
				Places: []*pb.RecogPlace{place},
			}
		} else {
			candidates.Places = append(candidates.Places, place)
		}
	}
	// Return the maximally matched span.
	candidates, ok := candidatesByNumTokens[numTokens]
	if !ok {
		return numTokens, &pb.RecogPlaces{}
	}
	return numTokens, candidates
}

func (p *placeRecognition) replaceTokensWithCandidates(tokens []string) *pb.TokenSpans {
	res := &pb.TokenSpans{}
	for len(tokens) > 0 {
		numTokens, candidates := p.findPlaceCandidates(tokens)
		if numTokens > 0 {
			res.Spans = append(res.Spans, &pb.TokenSpans_Span{
				Tokens: tokens[0:numTokens],
				Places: candidates.GetPlaces(),
			})
			tokens = tokens[numTokens:]
		} else {
			res.Spans = append(res.Spans, &pb.TokenSpans_Span{
				Tokens: tokens[0:1],
			})
			tokens = tokens[1:]
		}
	}
	return res
}

func getNumSpansForContainedIn(spans []*pb.TokenSpans_Span, startIdx int) int {
	size := len(spans)

	// Case: "place1, place2".
	if size > startIdx+2 && len(spans[startIdx+2].GetPlaces()) > 0 {
		nextSpanTokens := spans[startIdx+1].GetTokens()
		if len(nextSpanTokens) == 1 && nextSpanTokens[0] == "," {
			return 3
		}
	}

	// Case: "place1 place2"
	if size > startIdx+1 && len(spans[startIdx+1].GetPlaces()) > 0 {
		return 2
	}

	// Case: no contained in.
	return 0
}

func combineContainedInSingle(
	spans []*pb.TokenSpans_Span, startIdx, numSpans int) *pb.TokenSpans_Span {
	startSpan := spans[startIdx]
	endSpan := spans[startIdx+numSpans-1]

	res := &pb.TokenSpans_Span{Tokens: startSpan.Tokens}
	for i := 1; i < numSpans; i++ {
		res.Tokens = append(res.Tokens, spans[startIdx+i].GetTokens()...)
	}

	// This map is used to collect all the places for the combined span, with dedup.
	dcidToRecogPlaces := map[string]*pb.RecogPlace{}

	for _, p1 := range startSpan.GetPlaces() {
		for _, containingPlace := range p1.GetContainingPlaces() {
			for _, p2 := range endSpan.GetPlaces() {
				if containingPlace == p2.GetDcid() {
					dcidToRecogPlaces[p1.GetDcid()] = p1
				}
			}
		}
	}

	if len(dcidToRecogPlaces) > 0 {
		for _, p := range dcidToRecogPlaces {
			res.Places = append(res.Places, p)
		}
		return res
	}

	return nil
}

func combineContainedIn(tokenSpans *pb.TokenSpans) *pb.TokenSpans {
	spans := tokenSpans.GetSpans()
	i := 0
	res := &pb.TokenSpans{}
	for i < len(spans) {
		tokenSpan := spans[i]
		if len(tokenSpan.GetPlaces()) == 0 {
			i++
			res.Spans = append(res.Spans, tokenSpan)
			continue
		}

		numSpans := getNumSpansForContainedIn(spans, i)
		if numSpans == 0 {
			i++
			res.Spans = append(res.Spans, tokenSpan)
			continue
		}

		collapsedTokenSpan := combineContainedInSingle(spans, i, numSpans)
		if collapsedTokenSpan == nil {
			i++
			res.Spans = append(res.Spans, tokenSpan)
		} else {
			i += numSpans
			res.Spans = append(res.Spans, collapsedTokenSpan)
		}
	}

	return res
}

func (p *placeRecognition) rankAndTrimCandidates(
	tokenSpans *pb.TokenSpans,
	maxPlaceCandidates int) *pb.TokenSpans {
	res := &pb.TokenSpans{}
	for _, span := range tokenSpans.GetSpans() {
		if len(span.GetPlaces()) == 0 {
			res.Spans = append(res.Spans, span)
			continue
		}

		// Deal with bogus name (not followed by an ancestor place).
		spanStr := strings.ToLower(strings.Join(span.Tokens, " "))
		if _, ok := p.recogPlaceStore.BogusPlaceNames[spanStr]; ok && !p.resolveBogusName {
			span.Places = nil
			res.Spans = append(res.Spans, span)
			continue
		}

		// Deal with places that are adjectival and have suffixes.
		if _, ok := p.recogPlaceStore.AdjectivalNamesWithSuffix[spanStr]; ok && len(span.Tokens) > 1 && len(span.GetPlaces()) == 1 {
			// Create a new span with the suffix token
			newSpan := &pb.TokenSpans_Span{}
			newSpan.Tokens = span.Tokens[len(span.Tokens)-1:]

			// Drop the last token from the current span
			span.Tokens = span.Tokens[:len(span.Tokens)-1]

			// Add both to the result.
			res.Spans = append(res.Spans, span)
			res.Spans = append(res.Spans, newSpan)
			continue
		}

		// Rank by descending population.
		sort.SliceStable(span.Places, func(i, j int) bool {
			return span.Places[i].GetPopulation() > span.Places[j].GetPopulation()
		})

		if len(span.GetPlaces()) > maxPlaceCandidates {
			// Firstly, pick all place candidates with index < maxPlaceCandidates.
			filteredPlaces := span.Places[:maxPlaceCandidates]

			// For the rest, only pick the places with
			// population >= minPopulationOverMaxPlaceCandidates.
			for i := maxPlaceCandidates; i < len(span.Places); i++ {
				thisPlace := span.Places[i]
				if thisPlace.Population < minPopulationOverMaxPlaceCandidates {
					break
				}
				filteredPlaces = append(filteredPlaces, thisPlace)
			}

			span.Places = filteredPlaces
		}
		res.Spans = append(res.Spans, span)
	}
	return res
}

// Combine successive non-place tokens.
func formatResponse(
	query string, tokenSpans *pb.TokenSpans) *pb.RecognizePlacesResponse_Items {
	res := &pb.RecognizePlacesResponse_Items{}
	spanParts := []string{}
	for _, tokenSpan := range tokenSpans.GetSpans() {
		span := strings.Join(tokenSpan.GetTokens(), " ")
		if len(tokenSpan.GetPlaces()) > 0 {
			if len(spanParts) > 0 {
				res.Items = append(res.Items, &pb.RecognizePlacesResponse_Item{
					Span: strings.Join(spanParts, " "),
				})
				spanParts = []string{}
			}

			places := []*pb.RecognizePlacesResponse_Place{}
			for _, p := range tokenSpan.GetPlaces() {
				places = append(places, &pb.RecognizePlacesResponse_Place{
					Dcid: p.GetDcid(),
				})
			}

			res.Items = append(res.Items, &pb.RecognizePlacesResponse_Item{
				Span:   span,
				Places: places,
			})
		} else {
			spanParts = append(spanParts, span)
		}
	}

	if len(spanParts) > 0 {
		res.Items = append(res.Items, &pb.RecognizePlacesResponse_Item{
			Span: strings.Join(spanParts, " "),
		})
	}

	return res
}
