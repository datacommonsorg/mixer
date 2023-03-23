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
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store"
)

// RecognizePlaces implements API for ReconServer.RecognizePlaces.
func RecognizePlaces(
	ctx context.Context, in *pb.RecognizePlacesRequest, store *store.Store,
) (*pb.RecognizePlacesResponse, error) {
	// TODO(ws): Implement.
	return nil, nil
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

/*
   if (len(tokens) == 0):
      return None
    next = tokens[0].lower()
    if (next not in self.places):
      return [0, None]
    places = self.places[next]
    numTokens = 1
    candidates = []
    for cand in places:
      n = 0
      notFound = False
      for w in cand.words:
        if ((len(tokens) <= n) or (w != tokens[n].lower())):
          notFound = True
          break
        n = n + 1
      if (numTokens < n):
        numTokens = n
      if (not notFound):
        # Make a deep copy since we may mutate the candidates.
        candidates.append(copy.deepcopy(cand))
    return [numTokens, candidates]
*/

func findPlaceCandidates(
	tokens []string,
	recogPlaceMap map[string]*pb.RecogPlaces) (int, *pb.RecogPlaces) {
	if len(tokens) == 0 {
		return 0, nil
	}

	key := strings.ToLower(tokens[0])
	places, ok := recogPlaceMap[key]
	if !ok {
		return 0, nil
	}

	numTokens := 1
	candidates := &pb.RecogPlaces{}
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

			if nameMatched {
				matchedNameSize = namePartsSize
				break
			}
		}

		if matchedNameSize == 0 {
			continue
		}
		if numTokens < matchedNameSize {
			numTokens = matchedNameSize
		}
		candidates.Places = append(candidates.Places, place)
	}

	return numTokens, candidates
}
