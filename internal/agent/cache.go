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
	"slices"
	"sync"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
)

// Cache manages a high-performance, thread-safe in-memory cache of per-place data: Statistical
// Variable observations availability and child entity types.
type Cache struct {
	mixer Mixer
	mu    sync.RWMutex

	// Cache map: placeDcid -> Set of available variableDcids
	placeVars map[string]map[string]struct{}

	// Cache map: ancestorDcid -> child entity types (empty when it has none)
	childTypes map[string][]string
}

// NewCache constructs a new Cache instance backed by the Mixer client.
func NewCache(mixer Mixer) *Cache {
	return &Cache{
		mixer:      mixer,
		placeVars:  make(map[string]map[string]struct{}),
		childTypes: make(map[string][]string),
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

	uniqueMap, err := c.loadPlaceVariables(ctx, missingPlaces)
	if err != nil {
		return nil, err
	}

	// Populate final results directly
	for _, place := range missingPlaces {
		populateResult(place, variables, uniqueMap[place], result)
	}

	return result, nil
}

// EntitiesWithoutData returns, in request order, the entities that have no observation data for any
// variable. It uses the same per-entity variable sets as CheckAvailability, loading (and caching) the
// ones not cached yet, so a later CheckAvailability for these entities is served from L1. If any load
// fails, the error is returned and nothing from that load is cached.
func (c *Cache) EntitiesWithoutData(ctx context.Context, entities []string) ([]string, error) {
	cached := make(map[string]map[string]struct{}, len(entities))
	var missing []string
	c.mu.RLock()
	for _, entity := range entities {
		if vars, ok := c.placeVars[entity]; ok {
			cached[entity] = vars
		} else {
			missing = append(missing, entity)
		}
	}
	c.mu.RUnlock()

	if len(missing) > 0 {
		loaded, err := c.loadPlaceVariables(ctx, missing)
		if err != nil {
			return nil, err
		}
		for entity, vars := range loaded {
			cached[entity] = vars
		}
	}

	var withoutData []string
	for _, entity := range entities {
		if len(cached[entity]) == 0 && !slices.Contains(withoutData, entity) {
			withoutData = append(withoutData, entity)
		}
	}
	return withoutData, nil
}

// Reset flushes all cached place availability mappings atomically,
// called during background database reloads.
func (c *Cache) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.placeVars = make(map[string]map[string]struct{})
	c.childTypes = make(map[string][]string)
}

// ChildTypes returns the child entity types (types of entities contained in it via containedInPlace) of
// each ancestor, reading through the cache: ancestors not yet cached are fetched in one Mixer call and
// cached, including ancestors without child types.
// Fetch errors are returned and not cached, so a failed lookup is retried on the next call.
// The returned slices are shared and must not be modified.
func (c *Cache) ChildTypes(ctx context.Context, ancestors []string) (map[string][]string, error) {
	result := make(map[string][]string, len(ancestors))
	var missing []string
	c.mu.RLock()
	for _, ancestor := range ancestors {
		if types, ok := c.childTypes[ancestor]; ok {
			result[ancestor] = types
		} else {
			missing = append(missing, ancestor)
		}
	}
	c.mu.RUnlock()
	if len(missing) == 0 {
		return result, nil
	}

	fetched, err := fetchChildTypes(ctx, c.mixer, missing)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	for _, ancestor := range missing {
		// A concurrent call may have cached this ancestor meanwhile; keep its entry.
		if cached, ok := c.childTypes[ancestor]; ok {
			result[ancestor] = cached
			continue
		}
		types := fetched[ancestor]
		if types == nil {
			types = []string{}
		}
		c.childTypes[ancestor] = types
		result[ancestor] = types
	}
	c.mu.Unlock()
	return result, nil
}

// fetchChildTypes calls Mixer.ChildTypes for ancestors and returns child types keyed by ancestor.
// Ancestors are sent sorted so equal sets map to the same request (and Redis cache key).
func fetchChildTypes(ctx context.Context, mixer Mixer, ancestors []string) (map[string][]string, error) {
	resp, err := mixer.ChildTypes(ctx, &pbv2.ChildTypesRequest{Ancestors: slices.Sorted(slices.Values(ancestors))})
	if err != nil {
		return nil, err
	}
	childTypes := make(map[string][]string, len(resp.GetChildTypes()))
	for ancestor, types := range resp.GetChildTypes() {
		childTypes[ancestor] = types.GetTypes()
	}
	return childTypes, nil
}

// loadPlaceVariables fetches the available variables of each place in parallel (one V2Observation call
// per unique place) and warms the L1 cache with them. Results are returned keyed by place, so callers are
// unaffected by a concurrent Reset. If any fetch fails, nothing is cached and the error is returned.
func (c *Cache) loadPlaceVariables(ctx context.Context, places []string) (map[string]map[string]struct{}, error) {
	// Dynamic Cache Miss: Fetch available variables for missing places in parallel.
	// Deduplicate places to avoid redundant parallel fetches.
	uniqueMissing := util.StringSetToSlice(util.StringSliceToSet(places))

	// CONCURRENCY NOTE: We collect results in a local slice (`fetchedVars`) by index.
	// While Go maps are not thread-safe for parallel writes (even for disjoint keys),
	// writing to disjoint indices of a Go slice from parallel goroutines is 100% thread-safe
	// and guarantees no data races or lock contention during parallel fetches.
	// It also keeps the current request immune to any concurrent L1 cache map Reset() wipes.
	fetchedVars := make([]map[string]struct{}, len(uniqueMissing))
	g, groupCtx := errgroup.WithContext(ctx)

	for i, place := range uniqueMissing {
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

	// Map unique results for fast lookup
	uniqueMap := make(map[string]map[string]struct{})
	for i, place := range uniqueMissing {
		uniqueMap[place] = fetchedVars[i]
	}

	// Warm L1 cache map under write lock
	c.mu.Lock()
	for place, vars := range uniqueMap {
		c.placeVars[place] = vars
	}
	c.mu.Unlock()

	return uniqueMap, nil
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
