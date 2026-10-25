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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// mockV2ResolveEntitiesServer returns canned V2Resolve candidates keyed by resolveKey and records
// every resolve call so tests can assert which resolvers ran.
type mockV2ResolveEntitiesServer struct {
	Mixer
	resolveCandidates map[string][]*pbv2.ResolveResponse_Entity_Candidate
	resolverErrs      map[string]error // keyed by resolver name
	nodeData          map[string]*pbv2.LinkedGraph
	nodeErr           error               // applies to name/type V2Node lookups (including child type names), not the place-type lookup or ChildTypes
	placeTypesErr     error               // applies to the place-type lookup only
	childTypes        map[string][]string // ChildTypes response, keyed by ancestor
	childTypesErr     error
	childTypesCalls   [][]string

	mu    sync.Mutex
	calls []string
}

// resolveKey identifies a canned resolve result by resolver, property expression, and node.
func resolveKey(resolver, property, node string) string {
	return resolver + "|" + property + "|" + node
}

func (m *mockV2ResolveEntitiesServer) V2Resolve(ctx context.Context, in *pbv2.ResolveRequest) (*pbv2.ResolveResponse, error) {
	resp := &pbv2.ResolveResponse{}
	for _, node := range in.GetNodes() {
		key := resolveKey(in.GetResolver(), in.GetProperty(), node)
		m.mu.Lock()
		m.calls = append(m.calls, key)
		m.mu.Unlock()
		resp.Entities = append(resp.Entities, &pbv2.ResolveResponse_Entity{
			Node:       node,
			Candidates: m.resolveCandidates[key],
		})
	}
	if err := m.resolverErrs[in.GetResolver()]; err != nil {
		return nil, err
	}
	return resp, nil
}

func (m *mockV2ResolveEntitiesServer) V2Node(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	if in.GetProperty() == v2PlaceSubclassProperty {
		if m.placeTypesErr != nil {
			return nil, m.placeTypesErr
		}
		// Mirrors the legacy V2Node path, which rejects unfiltered "+" arcs unless diverted to Spanner
		// (X-Disable-Spanner overrides the divert).
		if !util.IsHeaderTrue(ctx, util.XDivertSpanner) || util.IsHeaderTrue(ctx, util.XDisableSpanner) {
			return nil, status.Error(codes.InvalidArgument, "unfiltered + arcs require Spanner")
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	} else if m.nodeErr != nil {
		return nil, m.nodeErr
	}
	resp := &pbv2.NodeResponse{Data: make(map[string]*pbv2.LinkedGraph)}
	for _, node := range in.GetNodes() {
		if graph, ok := m.nodeData[node]; ok {
			resp.Data[node] = graph
		}
	}
	return resp, nil
}

func (m *mockV2ResolveEntitiesServer) ChildTypes(ctx context.Context, in *pbv2.ChildTypesRequest) (*pbv2.ChildTypesResponse, error) {
	m.mu.Lock()
	m.childTypesCalls = append(m.childTypesCalls, in.GetAncestors())
	m.mu.Unlock()
	if m.childTypesErr != nil {
		return nil, m.childTypesErr
	}
	resp := &pbv2.ChildTypesResponse{ChildTypes: map[string]*pbv2.ChildTypesResponse_ChildTypes{}}
	for _, ancestor := range in.GetAncestors() {
		if types, ok := m.childTypes[ancestor]; ok {
			resp.ChildTypes[ancestor] = &pbv2.ChildTypesResponse_ChildTypes{Types: types}
		}
	}
	return resp, nil
}

// placeSubclassGraphs returns V2Node data for the place type roots, as returned for "<-subClassOf+".
func placeSubclassGraphs(subclasses ...string) map[string]*pbv2.LinkedGraph {
	nodes := make([]*pb.EntityInfo, 0, len(subclasses))
	for _, dcid := range subclasses {
		nodes = append(nodes, &pb.EntityInfo{Dcid: dcid})
	}
	return map[string]*pbv2.LinkedGraph{
		"AdministrativeArea": {Arcs: map[string]*pbv2.Nodes{"subClassOf+": {Nodes: nodes}}},
	}
}

// withPlaceSubclasses returns nodeData plus place-type root subclasses used by most tests.
func withPlaceSubclasses(nodeData map[string]*pbv2.LinkedGraph) map[string]*pbv2.LinkedGraph {
	merged := placeSubclassGraphs("Country", "State", "County", "City", "AdministrativeArea1")
	for k, v := range nodeData {
		merged[k] = v
	}
	return merged
}

// scoredType builds a type resolver candidate with a similarity score in its metadata.
func scoredType(dcid string, score string) *pbv2.ResolveResponse_Entity_Candidate {
	return &pbv2.ResolveResponse_Entity_Candidate{Dcid: dcid, Metadata: map[string]string{"score": score}}
}

// nodeGraph builds a LinkedGraph with a name and typeOf values for V2Node mocks.
func nodeGraph(name string, types ...string) *pbv2.LinkedGraph {
	typeNodes := make([]*pb.EntityInfo, 0, len(types))
	for _, t := range types {
		typeNodes = append(typeNodes, &pb.EntityInfo{Dcid: t})
	}
	return &pbv2.LinkedGraph{
		Arcs: map[string]*pbv2.Nodes{
			"name":   {Nodes: []*pb.EntityInfo{{Value: name}}},
			"typeOf": {Nodes: typeNodes},
		},
	}
}

func TestV2ResolveEntities(t *testing.T) {
	const unfiltered = PropDescription
	tests := []struct {
		name              string
		req               *V2ResolveEntitiesRequest
		resolveCandidates map[string][]*pbv2.ResolveResponse_Entity_Candidate
		resolverErrs      map[string]error
		nodeData          map[string]*pbv2.LinkedGraph
		nodeErr           error
		wantRows          [][]any
		wantCalls         []string
	}{
		{
			name: "place without hints uses place resolver and enriches name and types",
			req:  &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{{Name: "California"}}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverPlace, unfiltered, "California"): {{Dcid: "geoId/06"}},
			},
			nodeData: map[string]*pbv2.LinkedGraph{"geoId/06": nodeGraph("California", "State", "Place")},
			wantRows: [][]any{
				{"California", "geoId/06", "California", []string{"State"}},
			},
			wantCalls: []string{resolveKey(ResolverPlace, unfiltered, "California")},
		},
		{
			name: "place type hint filters place resolution by the hinted place type",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "Georgia", EntityTypeHints: []string{"country"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "country"):                                       {{Dcid: "Country"}, {Dcid: "CountryGroup"}},
				resolveKey(ResolverPlace, "<-description{typeOf:[Country]}->dcid", "Georgia"): {{Dcid: "country/GEO"}},
			},
			nodeData: map[string]*pbv2.LinkedGraph{"country/GEO": nodeGraph("Georgia", "Country", "Country")},
			wantRows: [][]any{
				{"Georgia", "country/GEO", "Georgia", []string{"Country"}},
			},
			wantCalls: []string{
				resolveKey(ResolverType, "", "country"),
				resolveKey(ResolverPlace, "<-description{typeOf:[Country]}->dcid", "Georgia"),
			},
		},
		{
			name: "hinted place type is not dropped when the filtered place search finds nothing",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "Santa Clara", EntityTypeHints: []string{"county"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "county"):                  {{Dcid: "County"}},
				resolveKey(ResolverPlace, unfiltered, "Santa Clara"):    {{Dcid: "geoId/0669084"}},
				resolveKey(ResolverNonPlace, unfiltered, "Santa Clara"): {{Dcid: "nces/1", TypeOf: []string{"PublicSchool"}}},
			},
			wantRows: [][]any{
				{"Santa Clara", "", "", []string{}},
			},
			wantCalls: []string{
				resolveKey(ResolverType, "", "county"),
				resolveKey(ResolverPlace, "<-description{typeOf:[County]}->dcid", "Santa Clara"),
				resolveKey(ResolverNonPlace, "<-description{typeOf:[County]}->dcid", "Santa Clara"),
			},
		},
		{
			name: "place query falls back to unfiltered non_place when place resolver finds nothing",
			req:  &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{{Name: "High income countries"}}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverNonPlace, unfiltered, "High income countries"): {
					{Dcid: "HighIncomeCountries", Name: "high income countries", TypeOf: []string{"CountryGroup"}},
				},
			},
			wantRows: [][]any{
				{"High income countries", "HighIncomeCountries", "high income countries", []string{"CountryGroup"}},
			},
			wantCalls: []string{
				resolveKey(ResolverPlace, unfiltered, "High income countries"),
				resolveKey(ResolverNonPlace, unfiltered, "High income countries"),
			},
		},
		{
			name: "non-place hint skips place resolver and strips types rejected by non_place",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "St. Mary's Food Bank", EntityTypeHints: []string{"food bank"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "food bank"): {{Dcid: "FoodBank"}, {Dcid: "Topic"}},
				resolveKey(ResolverNonPlace, "<-description{typeOf:[FoodBank]}->dcid", "St. Mary's Food Bank"): {
					{Dcid: "foodBank/stMarys", Name: "St Marys", TypeOf: []string{"FoodBank"}},
				},
			},
			nodeData: map[string]*pbv2.LinkedGraph{"foodBank/stMarys": nodeGraph("St. Mary's Food Bank", "FoodBank")},
			wantRows: [][]any{
				{"St. Mary's Food Bank", "foodBank/stMarys", "St. Mary's Food Bank", []string{"FoodBank"}},
			},
			wantCalls: []string{
				resolveKey(ResolverType, "", "food bank"),
				resolveKey(ResolverNonPlace, "<-description{typeOf:[FoodBank]}->dcid", "St. Mary's Food Bank"),
			},
		},
		{
			name: "non-place query falls back to unfiltered non_place when filtered search finds nothing",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "FEMA", EntityTypeHints: []string{"agency"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "agency"): {{Dcid: "GovernmentAgency"}},
				resolveKey(ResolverNonPlace, unfiltered, "FEMA"): {
					{Dcid: "fema", Name: "FEMA", TypeOf: []string{"Organization"}},
				},
			},
			wantRows: [][]any{
				{"FEMA", "fema", "FEMA", []string{"Organization"}},
			},
			wantCalls: []string{
				resolveKey(ResolverType, "", "agency"),
				resolveKey(ResolverNonPlace, "<-description{typeOf:[GovernmentAgency]}->dcid", "FEMA"),
				resolveKey(ResolverNonPlace, unfiltered, "FEMA"),
			},
		},
		{
			name: "non-place query tries the place resolver last",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "Golden Gate Bridge", EntityTypeHints: []string{"bridge"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "bridge"):                      {{Dcid: "Bridge"}},
				resolveKey(ResolverPlace, unfiltered, "Golden Gate Bridge"): {{Dcid: "bridge/ggb"}},
			},
			nodeData: map[string]*pbv2.LinkedGraph{"bridge/ggb": nodeGraph("Golden Gate Bridge", "Bridge")},
			wantRows: [][]any{
				{"Golden Gate Bridge", "bridge/ggb", "Golden Gate Bridge", []string{"Bridge"}},
			},
			wantCalls: []string{
				resolveKey(ResolverType, "", "bridge"),
				resolveKey(ResolverNonPlace, "<-description{typeOf:[Bridge]}->dcid", "Golden Gate Bridge"),
				resolveKey(ResolverNonPlace, unfiltered, "Golden Gate Bridge"),
				resolveKey(ResolverPlace, unfiltered, "Golden Gate Bridge"),
			},
		},
		{
			name: "candidates are deduplicated and capped per query",
			req:  &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{{Name: "Springfield"}}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverPlace, unfiltered, "Springfield"): {
					{Dcid: "a"}, {Dcid: "a"}, {Dcid: "b"}, {Dcid: "c"}, {Dcid: "d"}, {Dcid: "e"}, {Dcid: "f"},
				},
			},
			wantRows: [][]any{
				{"Springfield", "a", "", []string{}},
				{"Springfield", "b", "", []string{}},
				{"Springfield", "c", "", []string{}},
			},
			wantCalls: []string{resolveKey(ResolverPlace, unfiltered, "Springfield")},
		},
		{
			name: "place filter uses only the first non-generic place type",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "Georgia", EntityTypeHints: []string{"state"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "state"):                                       {{Dcid: "Place"}, {Dcid: "State"}, {Dcid: "AdministrativeArea1"}},
				resolveKey(ResolverPlace, "<-description{typeOf:[State]}->dcid", "Georgia"): {{Dcid: "geoId/13"}},
			},
			nodeData: map[string]*pbv2.LinkedGraph{"geoId/13": nodeGraph("Georgia", "State")},
			wantRows: [][]any{
				{"Georgia", "geoId/13", "Georgia", []string{"State"}},
			},
			wantCalls: []string{
				resolveKey(ResolverType, "", "state"),
				resolveKey(ResolverPlace, "<-description{typeOf:[State]}->dcid", "Georgia"),
			},
		},
		{
			name: "mixed hints route by the first hint and filter by all hinted types in hint order",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "Lincoln High", EntityTypeHints: []string{"school", "city"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "school"): {{Dcid: "School"}},
				resolveKey(ResolverType, "", "city"):   {{Dcid: "City"}},
				resolveKey(ResolverNonPlace, "<-description{typeOf:[School,City]}->dcid", "Lincoln High"): {
					{Dcid: "nces/1", Name: "Lincoln High School", TypeOf: []string{"School"}},
				},
			},
			wantRows: [][]any{
				{"Lincoln High", "nces/1", "Lincoln High School", []string{"School"}},
			},
			wantCalls: []string{
				resolveKey(ResolverType, "", "school"),
				resolveKey(ResolverType, "", "city"),
				resolveKey(ResolverNonPlace, "<-description{typeOf:[School,City]}->dcid", "Lincoln High"),
			},
		},
		{
			name:         "place resolver error falls back to non_place",
			req:          &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{{Name: "Middle East"}}},
			resolverErrs: map[string]error{ResolverPlace: errors.New("maps unavailable")},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverNonPlace, unfiltered, "Middle East"): {
					{Dcid: "MiddleEast", Name: "Middle East", TypeOf: []string{"GeoRegion"}},
				},
			},
			wantRows: [][]any{
				{"Middle East", "MiddleEast", "Middle East", []string{"GeoRegion"}},
			},
			wantCalls: []string{
				resolveKey(ResolverPlace, unfiltered, "Middle East"),
				resolveKey(ResolverNonPlace, unfiltered, "Middle East"),
			},
		},
		{
			name: "enrichment failure keeps resolver names and types",
			req:  &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{{Name: "  FEMA  "}}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverNonPlace, unfiltered, "FEMA"): {
					{Dcid: "fema", Name: "FEMA", TypeOf: []string{"Organization"}},
				},
			},
			nodeErr: errors.New("node lookup failed"),
			wantRows: [][]any{
				{"  FEMA  ", "fema", "FEMA", []string{"Organization"}},
			},
			wantCalls: []string{
				resolveKey(ResolverPlace, unfiltered, "FEMA"),
				resolveKey(ResolverNonPlace, unfiltered, "FEMA"),
			},
		},
		{
			name: "unresolved name returns a placeholder row",
			req:  &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{{Name: "Nowhereville"}}},
			wantRows: [][]any{
				{"Nowhereville", "", "", []string{}},
			},
			wantCalls: []string{
				resolveKey(ResolverPlace, unfiltered, "Nowhereville"),
				resolveKey(ResolverNonPlace, unfiltered, "Nowhereville"),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockV2ResolveEntitiesServer{
				resolveCandidates: tc.resolveCandidates,
				resolverErrs:      tc.resolverErrs,
				nodeData:          withPlaceSubclasses(tc.nodeData),
				nodeErr:           tc.nodeErr,
			}
			got, err := NewService(mock, nil, nil).V2ResolveEntities(context.Background(), tc.req)
			if err != nil {
				t.Fatalf("V2ResolveEntities() unexpected error: %v", err)
			}
			want := &V2ResolveEntitiesResponse{ResolvedEntities: &Table{
				Columns: []string{"query", "dcid", "name", "types"},
				Rows:    tc.wantRows,
			}}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("V2ResolveEntities() diff (-want +got):\n%s", diff)
			}
			// Each case has a single entity, so calls are sequential and their order is the resolution order.
			if diff := cmp.Diff(tc.wantCalls, mock.calls); diff != "" {
				t.Errorf("V2Resolve calls diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestV2ResolveEntitiesErrors(t *testing.T) {
	tests := []struct {
		name          string
		req           *V2ResolveEntitiesRequest
		resolverErrs  map[string]error
		wantCode      codes.Code
		wantErrSubstr string
	}{
		{
			name:          "nil request",
			req:           nil,
			wantErrSubstr: "'entities' cannot be empty",
		},
		{
			name:          "empty entities",
			req:           &V2ResolveEntitiesRequest{},
			wantErrSubstr: "'entities' cannot be empty",
		},
		{
			name:          "blank entity name",
			req:           &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{{Name: "California"}, {Name: "  "}}},
			wantErrSubstr: "entities[1].name cannot be empty",
		},
		{
			name: "too many child type hints",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "Kenya", ChildEntityTypeHints: []string{"county", "subcounty", "ward", "town"}},
			}},
			wantErrSubstr: "entities[0].child_entity_type_hints has 4 hints: use at most 3",
		},
		{
			name:          "invalid target",
			req:           &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{{Name: "California"}}, Target: "bogus"},
			wantErrSubstr: `invalid target "bogus"`,
		},
		{
			name:          "too many entities",
			req:           &V2ResolveEntitiesRequest{Entities: slices.Repeat([]*V2EntityQuery{{Name: "California"}}, v2MaxEntitiesPerRequest+1)},
			wantCode:      codes.InvalidArgument,
			wantErrSubstr: "entities per call and split the rest",
		},
		{
			name: "error is returned with its status code only when every attempt fails",
			req:  &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{{Name: "California"}}},
			resolverErrs: map[string]error{
				ResolverPlace:    errors.New("maps unavailable"),
				ResolverNonPlace: status.Error(codes.Unimplemented, "non_place not supported"),
			},
			wantCode:      codes.Unimplemented,
			wantErrSubstr: "non_place not supported",
		},
		{
			name:          "type hint failure is returned",
			req:           &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{{Name: "FEMA", EntityTypeHints: []string{"agency"}}}},
			resolverErrs:  map[string]error{ResolverType: errors.New("type backend down")},
			wantCode:      codes.Internal,
			wantErrSubstr: "type backend down",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockV2ResolveEntitiesServer{resolverErrs: tc.resolverErrs}
			_, err := NewService(mock, nil, nil).V2ResolveEntities(context.Background(), tc.req)
			if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
				t.Fatalf("V2ResolveEntities() error = %v, want substring %q", err, tc.wantErrSubstr)
			}
			wantCode := tc.wantCode
			if wantCode == codes.OK {
				wantCode = codes.InvalidArgument
			}
			if got := status.Code(err); got != wantCode {
				t.Errorf("V2ResolveEntities() code = %v, want %v", got, wantCode)
			}
		})
	}
}

func TestV2PlaceTypes(t *testing.T) {
	ctx := context.Background()
	mock := &mockV2ResolveEntitiesServer{placeTypesErr: errors.New("node lookup failed")}
	service := NewService(mock, nil, nil)
	isPlace := func(dcid string) bool {
		_, ok := service.v2PlaceTypes(ctx)[dcid]
		return ok
	}

	if !isPlace("AdministrativeArea") || !isPlace("Town") || isPlace("Country") {
		t.Errorf("on lookup failure, want only roots and extras")
	}

	mock.placeTypesErr = nil
	mock.nodeData = placeSubclassGraphs("Country")
	if isPlace("Country") {
		t.Errorf("within the retry interval after a failure, want no reload")
	}

	service.placeTypes.lastFailure = time.Time{}
	mock.nodeData = nil
	if isPlace("Country") {
		t.Errorf("want an empty subclass response treated as a failure, not cached")
	}

	service.placeTypes.lastFailure = time.Time{}
	mock.nodeData = placeSubclassGraphs("Country")
	if !isPlace("Country") {
		t.Errorf("after the retry interval, want subclasses loaded")
	}

	mock.nodeData = placeSubclassGraphs("State")
	if isPlace("State") {
		t.Errorf("want loaded place types to be cached until Reset")
	}

	service.Reset()
	if !isPlace("State") || isPlace("Country") {
		t.Errorf("after Reset, want place types reloaded")
	}

	service.Reset()
	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	if _, ok := service.v2PlaceTypes(cancelledCtx)["State"]; !ok {
		t.Errorf("want the shared lookup to ignore the triggering request's cancellation")
	}

	service.Reset()
	disableCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(util.XDisableSpanner, "true"))
	if _, ok := service.v2PlaceTypes(disableCtx)["State"]; !ok {
		t.Errorf("want the shared lookup to ignore the triggering request's X-Disable-Spanner header")
	}
}

func TestResolutionPlan(t *testing.T) {
	placeTypes := map[string]struct{}{"Place": {}, "AdministrativeArea": {}, "Country": {}, "State": {}}
	tests := []struct {
		name      string
		typeDcids []string
		want      []resolveAttempt
	}{
		{
			name: "no hints is a place query without a filter",
			want: []resolveAttempt{{resolver: ResolverPlace}, {resolver: ResolverNonPlace}},
		},
		{
			name:      "generic-only place hint is a place query without a filter",
			typeDcids: []string{"Place"},
			want:      []resolveAttempt{{resolver: ResolverPlace}, {resolver: ResolverNonPlace}},
		},
		{
			name:      "place hint filters by the first non-generic place type",
			typeDcids: []string{"Place", "AdministrativeArea", "State", "Country"},
			want: []resolveAttempt{
				{resolver: ResolverPlace, typeDcids: []string{"State"}},
				{resolver: ResolverNonPlace, typeDcids: []string{"State", "Country"}},
			},
		},
		{
			name:      "non-place hint tries filtered and unfiltered non_place, then place last",
			typeDcids: []string{"School", "Topic", "City"},
			want: []resolveAttempt{
				{resolver: ResolverNonPlace, typeDcids: []string{"School", "City"}},
				{resolver: ResolverNonPlace},
				{resolver: ResolverPlace},
			},
		},
		{
			name:      "non-place hint with only excluded types skips the filtered attempt",
			typeDcids: []string{"Topic"},
			want:      []resolveAttempt{{resolver: ResolverNonPlace}, {resolver: ResolverPlace}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := resolutionPlan(tc.typeDcids, placeTypes)
			if diff := cmp.Diff(tc.want, got, cmp.AllowUnexported(resolveAttempt{})); diff != "" {
				t.Errorf("resolutionPlan() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBuildResolvedEntitiesTable(t *testing.T) {
	groups := []*v2EntityGroup{
		{query: "Georgia", entities: []*v2ResolvedEntity{
			{dcid: "country/GEO", name: "Georgia", types: []string{"Country"}},
			{dcid: "geoId/13", name: "Georgia", types: []string{"State"}},
		}},
		{query: "Nowhereville", entities: []*v2ResolvedEntity{}},
	}
	want := &Table{
		Columns: []string{"query", "dcid", "name", "types"},
		Rows: [][]any{
			{"Georgia", "country/GEO", "Georgia", []string{"Country"}},
			{"Georgia", "geoId/13", "Georgia", []string{"State"}},
			{"Nowhereville", "", "", []string{}},
		},
	}
	if diff := cmp.Diff(want, buildResolvedEntitiesTable(groups)); diff != "" {
		t.Errorf("buildResolvedEntitiesTable() diff (-want +got):\n%s", diff)
	}
}

func TestV2ResolveEntitiesMultipleEntities(t *testing.T) {
	ctx := context.Background()
	unfiltered := PropDescription

	t.Run("rows follow request order", func(t *testing.T) {
		mock := &mockV2ResolveEntitiesServer{
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverPlace, unfiltered, "India"):      {{Dcid: "country/IND"}},
				resolveKey(ResolverPlace, unfiltered, "California"): {{Dcid: "geoId/06"}},
			},
			nodeData: withPlaceSubclasses(map[string]*pbv2.LinkedGraph{
				"country/IND": nodeGraph("India", "Country"),
				"geoId/06":    nodeGraph("California", "State"),
			}),
		}
		got, err := NewService(mock, nil, nil).V2ResolveEntities(ctx, &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
			{Name: "India"}, {Name: "Nowhereville"}, {Name: "California"},
		}})
		if err != nil {
			t.Fatalf("V2ResolveEntities() unexpected error: %v", err)
		}
		wantRows := [][]any{
			{"India", "country/IND", "India", []string{"Country"}},
			{"Nowhereville", "", "", []string{}},
			{"California", "geoId/06", "California", []string{"State"}},
		}
		if diff := cmp.Diff(wantRows, got.ResolvedEntities.Rows); diff != "" {
			t.Errorf("rows diff (-want +got):\n%s", diff)
		}
	})

	t.Run("a name whose attempts all fail fails the call with its status code", func(t *testing.T) {
		mock := &mockV2ResolveEntitiesServer{
			resolverErrs: map[string]error{
				ResolverPlace:    status.Error(codes.Unavailable, "maps unavailable"),
				ResolverNonPlace: status.Error(codes.Unavailable, "embeddings unavailable"),
			},
		}
		_, err := NewService(mock, nil, nil).V2ResolveEntities(ctx, &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
			{Name: "India"}, {Name: "California"},
		}})
		if got := status.Code(err); got != codes.Unavailable {
			t.Errorf("V2ResolveEntities() code = %v, want %v (err: %v)", got, codes.Unavailable, err)
		}
	})
}

func TestV2ResolveEntitiesChildTypes(t *testing.T) {
	const unfiltered = PropDescription
	childColumns := []string{"query", "entity_dcid", "child_entity_type", "child_entity_type_name"}
	californiaChildTypes := []string{"AdministrativeArea", "AdministrativeArea2", "CensusCountyDivision", "CensusTract", "City", "County", "Place", "PublicSchool"}
	tests := []struct {
		name              string
		req               *V2ResolveEntitiesRequest
		resolveCandidates map[string][]*pbv2.ResolveResponse_Entity_Candidate
		childTypes        map[string][]string
		childTypesErr     error
		nodeData          map[string]*pbv2.LinkedGraph
		nodeErr           error
		wantChildRows     [][]any
		wantNoTable       bool
		wantNote          string
		wantChildCalls    [][]string
	}{
		{
			name: "each hint returns its best existing non-generic class",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "California", ChildEntityTypeHints: []string{"county", "administrative area 2", "region"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "county"):                {scoredType("County", "0.7493"), scoredType("Municipality", "0.6285"), scoredType("CensusCountyDivision", "0.6081")},
				resolveKey(ResolverType, "", "administrative area 2"): {scoredType("AdministrativeArea2", "0.7320"), scoredType("AdministrativeArea", "0.7172"), scoredType("AdministrativeArea5", "0.6658")},
				resolveKey(ResolverType, "", "region"):                {scoredType("AdministrativeArea", "0.70"), scoredType("Place", "0.69")},
				resolveKey(ResolverPlace, unfiltered, "California"):   {{Dcid: "geoId/06"}, {Dcid: "geoId/2412150"}},
			},
			childTypes: map[string][]string{"geoId/06": californiaChildTypes},
			nodeData: map[string]*pbv2.LinkedGraph{
				"County":              nodeGraph("County"),
				"AdministrativeArea2": nodeGraph("Administrative Area 2"),
			},
			wantChildRows: [][]any{
				{"California", "geoId/06", "County", "County"},
				{"California", "geoId/06", "AdministrativeArea2", "Administrative Area 2"},
			},
			wantChildCalls: [][]string{{"geoId/06"}},
		},
		{
			name: "near-tied existing classes are all returned for umbrella hints",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "California", ChildEntityTypeHints: []string{"school"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "school"):              {scoredType("HighSchool", "0.6618"), scoredType("ElementarySchool", "0.6599"), scoredType("MiddleSchool", "0.6455")},
				resolveKey(ResolverPlace, unfiltered, "California"): {{Dcid: "geoId/06"}},
			},
			childTypes: map[string][]string{"geoId/06": {"ElementarySchool", "HighSchool", "MiddleSchool"}},
			wantChildRows: [][]any{
				{"California", "geoId/06", "HighSchool", ""},
				{"California", "geoId/06", "ElementarySchool", ""},
				{"California", "geoId/06", "MiddleSchool", ""},
			},
			wantChildCalls: [][]string{{"geoId/06"}},
		},
		{
			name: "no matching child type returns a placeholder row with the parent",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "California", ChildEntityTypeHints: []string{"hospital"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "hospital"):            {{Dcid: "Hospital"}},
				resolveKey(ResolverPlace, unfiltered, "California"): {{Dcid: "geoId/06"}},
			},
			childTypes:     map[string][]string{"geoId/06": californiaChildTypes},
			wantChildRows:  [][]any{{"California", "geoId/06", "", ""}},
			wantChildCalls: [][]string{{"geoId/06"}},
		},
		{
			name: "child type lookup failure omits the child types table",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "California", ChildEntityTypeHints: []string{"county"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "county"):              {{Dcid: "County"}},
				resolveKey(ResolverPlace, unfiltered, "California"): {{Dcid: "geoId/06"}},
			},
			childTypesErr:  errors.New("lookup failed"),
			wantNoTable:    true,
			wantNote:       v2ChildTypesUnavailableNote,
			wantChildCalls: [][]string{{"geoId/06"}},
		},
		{
			name: "child type ranked below the entity type cap is still matched",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "Canada", ChildEntityTypeHints: []string{"province"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "province"): {
					scoredType("AdministrativeArea", "0.74"), scoredType("State", "0.73"),
					scoredType("Region", "0.72"), scoredType("Province", "0.71"),
				},
				resolveKey(ResolverPlace, unfiltered, "Canada"): {{Dcid: "country/CAN"}},
			},
			childTypes:     map[string][]string{"country/CAN": {"AdministrativeArea", "Province"}},
			wantChildRows:  [][]any{{"Canada", "country/CAN", "Province", ""}},
			wantChildCalls: [][]string{{"country/CAN"}},
		},
		{
			name: "unresolved entity with child hints returns a placeholder row without a parent and no lookup",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "Nowhereville", ChildEntityTypeHints: []string{"county"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "county"): {{Dcid: "County"}},
			},
			wantChildRows: [][]any{{"Nowhereville", "", "", ""}},
		},
		{
			name: "name lookup failure keeps matched child types without names",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "California", ChildEntityTypeHints: []string{"county"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "county"):              {{Dcid: "County"}},
				resolveKey(ResolverPlace, unfiltered, "California"): {{Dcid: "geoId/06"}},
			},
			childTypes:     map[string][]string{"geoId/06": californiaChildTypes},
			nodeErr:        errors.New("node lookup failed"),
			wantChildRows:  [][]any{{"California", "geoId/06", "County", ""}},
			wantChildCalls: [][]string{{"geoId/06"}},
		},
		{
			name: "child types of all parents are fetched in one call; only queries with non-blank hints get rows",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "India"},
				{Name: "Mexico", ChildEntityTypeHints: []string{" "}},
				{Name: "Kenya", ChildEntityTypeHints: []string{"county"}},
				{Name: "California", ChildEntityTypeHints: []string{"county"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "county"):              {scoredType("County", "0.75"), scoredType("AdministrativeArea1", "0.60")},
				resolveKey(ResolverPlace, unfiltered, "India"):      {{Dcid: "country/IND"}},
				resolveKey(ResolverPlace, unfiltered, "Mexico"):     {{Dcid: "country/MEX"}},
				resolveKey(ResolverPlace, unfiltered, "Kenya"):      {{Dcid: "country/KEN"}},
				resolveKey(ResolverPlace, unfiltered, "California"): {{Dcid: "geoId/06"}},
			},
			childTypes: map[string][]string{
				"country/KEN": {"AdministrativeArea1", "City", "County"},
				"geoId/06":    californiaChildTypes,
			},
			wantChildRows: [][]any{
				{"Kenya", "country/KEN", "County", ""},
				{"California", "geoId/06", "County", ""},
			},
			wantChildCalls: [][]string{{"country/KEN", "geoId/06"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockV2ResolveEntitiesServer{
				resolveCandidates: tc.resolveCandidates,
				childTypes:        tc.childTypes,
				childTypesErr:     tc.childTypesErr,
				nodeData:          withPlaceSubclasses(tc.nodeData),
				nodeErr:           tc.nodeErr,
			}
			got, err := NewService(mock, nil, nil).V2ResolveEntities(context.Background(), tc.req)
			if err != nil {
				t.Fatalf("V2ResolveEntities() unexpected error: %v", err)
			}
			want := &Table{Columns: childColumns, Rows: tc.wantChildRows}
			if tc.wantNoTable {
				want = nil
			}
			if diff := cmp.Diff(want, got.CandidateChildEntityTypes); diff != "" {
				t.Errorf("CandidateChildEntityTypes diff (-want +got):\n%s", diff)
			}
			if got.ChildEntityTypesNote != tc.wantNote {
				t.Errorf("ChildEntityTypesNote = %q, want %q", got.ChildEntityTypesNote, tc.wantNote)
			}
			if diff := cmp.Diff(tc.wantChildCalls, mock.childTypesCalls); diff != "" {
				t.Errorf("ChildTypes calls diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMatchChildTypes(t *testing.T) {
	tests := []struct {
		name        string
		hints       []string
		typesByHint map[string][]typeCandidate
		available   []string
		want        []string
	}{
		{
			name:        "best existing class per hint, skipping missing and generic classes",
			hints:       []string{"administrative area 1"},
			typesByHint: map[string][]typeCandidate{"administrative area 1": {{"AdministrativeArea", 0.7377}, {"Province", 0.73}, {"AdministrativeArea1", 0.7183}}},
			available:   []string{"AdministrativeArea", "AdministrativeArea1"},
			want:        []string{"AdministrativeArea1"},
		},
		{
			name:        "classes within the margin of the best existing class are kept, capped per hint",
			hints:       []string{"school"},
			typesByHint: map[string][]typeCandidate{"school": {{"HighSchool", 0.66}, {"ElementarySchool", 0.65}, {"MiddleSchool", 0.645}}},
			available:   []string{"HighSchool", "ElementarySchool", "MiddleSchool"},
			want:        []string{"HighSchool", "ElementarySchool", "MiddleSchool"},
		},
		{
			name:        "classes beyond the margin are dropped",
			hints:       []string{"county"},
			typesByHint: map[string][]typeCandidate{"county": {{"County", 0.75}, {"CensusCountyDivision", 0.70}}},
			available:   []string{"County", "CensusCountyDivision"},
			want:        []string{"County"},
		},
		{
			name:  "duplicates across hints are returned once, in hint order",
			hints: []string{"state", "province"},
			typesByHint: map[string][]typeCandidate{
				"state":    {{"State", 0.72}},
				"province": {{"Province", 0.71}, {"State", 0.70}},
			},
			available: []string{"State", "Province"},
			want:      []string{"State", "Province"},
		},
		{
			name:        "nothing available returns nothing",
			hints:       []string{"county"},
			typesByHint: map[string][]typeCandidate{"county": {{"County", 0.75}}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := matchChildTypes(tc.hints, tc.typesByHint, tc.available)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("matchChildTypes() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestV2ResolveEntitiesWithoutChildHintsOmitsChildTable(t *testing.T) {
	mock := &mockV2ResolveEntitiesServer{
		resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
			resolveKey(ResolverPlace, PropDescription, "California"): {{Dcid: "geoId/06"}},
		},
	}
	got, err := NewService(mock, nil, nil).V2ResolveEntities(context.Background(), &V2ResolveEntitiesRequest{
		Entities: []*V2EntityQuery{{Name: "California"}},
	})
	if err != nil {
		t.Fatalf("V2ResolveEntities() unexpected error: %v", err)
	}
	if got.CandidateChildEntityTypes != nil {
		t.Errorf("CandidateChildEntityTypes = %v, want nil", got.CandidateChildEntityTypes)
	}
}

func TestTypesForHints(t *testing.T) {
	typesByHint := map[string][]typeCandidate{
		"school":     {{"School", 0.7}, {"PublicSchool", 0.69}, {"HighSchool", 0.68}, {"PrivateSchool", 0.67}},
		"university": {{"CollegeOrUniversity", 0.8}, {"School", 0.6}},
	}
	tests := []struct {
		name  string
		hints []string
		want  []string
	}{
		{
			name:  "keeps the top v2MaxTypesPerHint classes per hint",
			hints: []string{"school"},
			want:  []string{"School", "PublicSchool", "HighSchool"},
		},
		{
			name:  "deduplicates across hints in hint order",
			hints: []string{"university", " school "},
			want:  []string{"CollegeOrUniversity", "School", "PublicSchool", "HighSchool"},
		},
		{
			name:  "unknown hints contribute nothing",
			hints: []string{"spaceship"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if diff := cmp.Diff(tc.want, typesForHints(tc.hints, typesByHint)); diff != "" {
				t.Errorf("typesForHints() diff (-want +got):\n%s", diff)
			}
		})
	}
}
