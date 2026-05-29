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

package agent

import (
	"context"
	"sync"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"golang.org/x/sync/errgroup"
)

// Cache manages a high-performance, thread-safe in-memory cache
// of Statistical Variable observations availability for places.
type Cache struct {
	mixer Mixer
	mu    sync.RWMutex

	// Cache map: placeDcid -> Set of available variableDcids
	placeVars map[string]map[string]struct{}
}

// NewCache constructs a new Cache instance backed by the Mixer client.
func NewCache(mixer Mixer) *Cache {
	return &Cache{
		mixer:     mixer,
		placeVars: make(map[string]map[string]struct{}),
	}
}

// CheckAvailability verifies the observation data existence for a list of candidate variables
// at a set of target places. It returns a map of place DCID to its matched variables availability map,
// utilizing a modular, high-performance read-through cache to dynamically load all variables
// for missing places in parallel.
func (c *Cache) CheckAvailability(
	ctx context.Context,
	places []string,
	variables []string,
) (map[string]map[string]bool, error) {
	result := make(map[string]map[string]bool)

	// Check local cache under read lock
	missingPlaces := c.checkLocalCache(places, variables, result)
	if len(missingPlaces) == 0 {
		return result, nil
	}

	// Dynamic Cache Miss: Fetch available variables for missing places in parallel.
	// CONCURRENCY NOTE: We collect results in a local slice (`fetchedVars`) by index.
	// While Go maps are not thread-safe for parallel writes (even for disjoint keys),
	// writing to disjoint indices of a Go slice from parallel goroutines is 100% thread-safe
	// and guarantees no data races or lock contention during parallel fetches.
	// It also keeps the current request immune to any concurrent L1 cache map Reset() wipes.
	fetchedVars := make([]map[string]struct{}, len(missingPlaces))
	g, groupCtx := errgroup.WithContext(ctx)

	for i, place := range missingPlaces {
		idx := i
		p := place // Pin loop variable for goroutine closure
		g.Go(func() error {
			vars, err := c.fetchPlaceAvailableVariables(groupCtx, p)
			if err != nil {
				return err
			}
			fetchedVars[idx] = vars
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Populate final results directly from the local fetched slice
	for i, place := range missingPlaces {
		populateResult(place, variables, fetchedVars[i], result)
	}

	// Warm L1 cache map under write lock
	c.mu.Lock()
	for i, place := range missingPlaces {
		c.placeVars[place] = fetchedVars[i]
	}
	c.mu.Unlock()

	return result, nil
}

// Reset flushes all cached place availability mappings atomically,
// called during background database reloads.
func (c *Cache) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.placeVars = make(map[string]map[string]struct{})
}

// checkLocalCache queries the local cache for a list of places under read lock.
// It populates the provided result map and returns the slice of missing places.
func (c *Cache) checkLocalCache(
	places []string,
	variables []string,
	result map[string]map[string]bool,
) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var missing []string
	for _, p := range places {
		if cachedSet, ok := c.placeVars[p]; ok {
			populateResult(p, variables, cachedSet, result)
		} else {
			missing = append(missing, p)
		}
	}
	return missing
}

// fetchPlaceAvailableVariables calls V2Observation to fetch all available variables for a single place.
// Queries are made per-place separately to maximize Redis L2 cache hit rates.
func (c *Cache) fetchPlaceAvailableVariables(ctx context.Context, placeDcid string) (map[string]struct{}, error) {
	// Wildcard facet-only V2 Observation request to fetch all available variables
	req := &pbv2.ObservationRequest{
		Entity: &pbv2.DcidOrExpression{
			Dcids: []string{placeDcid},
		},
		// Exclude date and value to perform a lightweight, metadata-only existence check
		Select: []string{"variable", "entity", "facet"},
	}

	resp, err := c.mixer.V2Observation(ctx, req)
	if err != nil {
		return nil, err
	}

	// Parse variables into a local set map
	availableVars := make(map[string]struct{})
	for v := range resp.GetByVariable() {
		availableVars[v] = struct{}{}
	}

	return availableVars, nil
}

// populateResult is a pure helper function that extracts variable availabilities
// from a source availableVariables set and populates the output result map.
func populateResult(
	place string,
	variables []string,
	availableVariables map[string]struct{},
	result map[string]map[string]bool,
) {
	if _, exists := result[place]; !exists {
		result[place] = make(map[string]bool)
	}
	for _, v := range variables {
		_, hasData := availableVariables[v]
		result[place][v] = hasData
	}
}
