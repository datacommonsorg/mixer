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
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/metadata"
)

// v2PlaceTypeRoots are the classes whose subclasses (looked up in the graph) are treated as geographic
// places that the place resolver handles well. "subClassOf Place" is deliberately not used: it also
// includes facilities such as schools, hospitals, churches and airports, which resolve better via
// non_place. These roots cover administrative areas (Country, State, County, City, AdministrativeArea1-5,
// ...), statistical areas (CensusTract, ZCTA, EurostatNUTS1-3, ...) and UN/continental regions.
var v2PlaceTypeRoots = []string{
	"AdministrativeArea",
	"StatisticalArea",
	"GeoRegion",
}

// v2PlaceTypeExtras are place types outside v2PlaceTypeRoots that sit directly under Place
// (or elsewhere) in the class hierarchy but should still route to the place resolver.
var v2PlaceTypeExtras = []string{
	"Place",
	"Continent",
	"Town",
	"Village",
}

const (
	// v2PlaceSubclassProperty fetches all transitive subclasses of a class.
	v2PlaceSubclassProperty = "<-subClassOf+"
	// v2PlaceTypesRetryInterval throttles reloading place types after a failed lookup, so a slow or
	// failing backend does not add an RPC (and lock wait) to every request.
	v2PlaceTypesRetryInterval = time.Minute
	// v2PlaceTypesLoadTimeout bounds the lookup, which runs independently of the triggering request.
	v2PlaceTypesLoadTimeout = 10 * time.Second
)

// placeTypeCache lazily loads and caches the set of place types. The zero value is ready to use.
type placeTypeCache struct {
	mu          sync.Mutex
	types       map[string]struct{}
	lastFailure time.Time
}

// v2PlaceTypes returns the set of Class DCIDs treated as geographic places, loading it from the graph on
// first use. If the lookup fails (or returns no subclasses), the roots and extras alone are returned and
// the lookup is retried after v2PlaceTypesRetryInterval, so a transient failure does not stick.
// The returned set is shared and must not be modified.
//
// The lock is not held during the lookup so concurrent requests are never blocked on it. Concurrent
// cold-start requests may each run the lookup; that is rare and cheaper than coordinating them.
func (s *Service) v2PlaceTypes(ctx context.Context) map[string]struct{} {
	s.placeTypes.mu.Lock()
	cached, lastFailure := s.placeTypes.types, s.placeTypes.lastFailure
	s.placeTypes.mu.Unlock()
	if cached != nil {
		return cached
	}
	types := baseV2PlaceTypes()
	if time.Since(lastFailure) < v2PlaceTypesRetryInterval {
		return types
	}

	subclasses, err := s.fetchPlaceSubclasses(ctx)
	if err == nil && len(subclasses) == 0 {
		// An empty hierarchy would silently route "country"/"state" hints to non_place, so it is
		// treated as a failure rather than cached.
		err = errors.New("no place subclasses returned")
	}

	s.placeTypes.mu.Lock()
	defer s.placeTypes.mu.Unlock()
	if s.placeTypes.types != nil {
		return s.placeTypes.types
	}
	if err != nil {
		slog.Warn("Failed to load place subclasses; using root place types only", "error", err)
		s.placeTypes.lastFailure = time.Now()
		return types
	}
	for _, dcid := range subclasses {
		types[dcid] = struct{}{}
	}
	s.placeTypes.types = types
	return types
}

// reset clears the cached place types so they are reloaded on next use.
func (c *placeTypeCache) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.types = nil
	c.lastFailure = time.Time{}
}

// isGenericPlaceType reports whether a place type is used for routing but never as a place resolver
// filter. The place resolver only keeps candidates whose direct typeOf matches the filter, and real
// places are typed Country/State/etc. rather than these broad classes, so filtering by them would drop
// every candidate.
func isGenericPlaceType(dcid string) bool {
	return dcid == "Place" || slices.Contains(v2PlaceTypeRoots, dcid)
}

// baseV2PlaceTypes returns the roots and extras as a new set.
func baseV2PlaceTypes() map[string]struct{} {
	types := make(map[string]struct{}, len(v2PlaceTypeRoots)+len(v2PlaceTypeExtras))
	for _, dcid := range slices.Concat(v2PlaceTypeRoots, v2PlaceTypeExtras) {
		types[dcid] = struct{}{}
	}
	return types
}

// fetchPlaceSubclasses returns all transitive subclasses of v2PlaceTypeRoots via one V2Node call.
//
// The lookup is detached from the triggering request's cancellation (with its own timeout) because the
// result is shared: one client disconnecting must not cause a cached failure for everyone. It is also
// forced onto the Spanner backend, since the legacy V2Node path rejects unfiltered "+" arcs such as
// "<-subClassOf+".
func (s *Service) fetchPlaceSubclasses(ctx context.Context) ([]string, error) {
	lookupCtx, cancel := context.WithTimeout(withSpannerDivert(context.WithoutCancel(ctx)), v2PlaceTypesLoadTimeout)
	defer cancel()
	resp, err := s.fetchAllNodes(lookupCtx, &pbv2.NodeRequest{Nodes: v2PlaceTypeRoots, Property: v2PlaceSubclassProperty})
	if err != nil {
		return nil, err
	}
	var subclasses []string
	for _, root := range v2PlaceTypeRoots {
		subclasses = append(subclasses, getPropDcids(resp.GetData()[root], "subClassOf+")...)
	}
	return subclasses, nil
}

// withSpannerDivert returns ctx with incoming metadata containing only X-Divert-Spanner. The triggering
// caller's metadata is deliberately not carried over: the result is shared, so it must not depend on
// per-caller headers (e.g. X-Disable-Spanner would override the divert and cache a failure for everyone).
func withSpannerDivert(ctx context.Context) context.Context {
	return metadata.NewIncomingContext(ctx, metadata.Pairs(util.XDivertSpanner, "true"))
}
