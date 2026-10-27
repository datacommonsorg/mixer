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
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mockV2SearchIndicatorsServer fakes the Mixer calls V2SearchIndicators makes, from a small topic tree.
type mockV2SearchIndicatorsServer struct {
	Mixer
	// topics maps a topic DCID to its name and direct members (topic or variable DCIDs).
	topics map[string]mockTopic
	// rootTopics are the top-level topic DCIDs.
	rootTopics []string
	// variableNames maps variable DCIDs to names.
	variableNames map[string]string
	// searchResults maps a query to the indicator resolver's ranked topic and variable DCIDs.
	searchResults map[string][]string
	// entityVariables maps an entity DCID to the variables with data for it.
	entityVariables map[string][]string
	// entityTypes maps an entity DCID to its types; entities without types don't exist.
	entityTypes map[string][]string
	// errs fails calls by method name ("indicator", "topic", "V2Observation", "V2Node").
	errs map[string]error
	// reverseTopicEntities returns topic resolver entities in reverse request order.
	reverseTopicEntities bool

	mu        sync.Mutex
	nodeCalls int
}

// mockTopic is a topic in mockV2SearchIndicatorsServer.
type mockTopic struct {
	name    string
	members []string
}

func (m *mockV2SearchIndicatorsServer) V2Resolve(ctx context.Context, in *pbv2.ResolveRequest) (*pbv2.ResolveResponse, error) {
	if err := m.errs[in.GetResolver()]; err != nil {
		return nil, err
	}
	switch {
	case in.GetResolver() == ResolverIndicator:
		var candidates []*pbv2.ResolveResponse_Entity_Candidate
		for _, dcid := range m.searchResults[in.GetNodes()[0]] {
			candidates = append(candidates, m.candidate(dcid, true))
		}
		return &pbv2.ResolveResponse{Entities: []*pbv2.ResolveResponse_Entity{{Node: in.GetNodes()[0], Candidates: candidates}}}, nil
	case len(in.GetNodes()) == 0:
		var roots []*pbv2.ResolveResponse_Entity_Candidate
		for _, dcid := range m.rootTopics {
			roots = append(roots, m.candidate(dcid, true))
		}
		return &pbv2.ResolveResponse{Entities: []*pbv2.ResolveResponse_Entity{{Candidates: roots}}}, nil
	default:
		resp := &pbv2.ResolveResponse{}
		for _, node := range in.GetNodes() {
			entity := &pbv2.ResolveResponse_Entity{Node: node}
			if _, ok := m.topics[node]; ok {
				topic := m.candidate(node, !in.GetExpandTopics())
				if in.GetExpandTopics() {
					for _, varDcid := range m.descendants(node) {
						topic.Children = append(topic.Children, m.candidate(varDcid, false))
					}
				}
				entity.Candidates = []*pbv2.ResolveResponse_Entity_Candidate{topic}
			}
			resp.Entities = append(resp.Entities, entity)
		}
		if m.reverseTopicEntities {
			slices.Reverse(resp.Entities)
		}
		return resp, nil
	}
}

func (m *mockV2SearchIndicatorsServer) V2Observation(ctx context.Context, in *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
	if err := m.errs["V2Observation"]; err != nil {
		return nil, err
	}
	resp := &pbv2.ObservationResponse{ByVariable: map[string]*pbv2.VariableObservation{}}
	for _, varDcid := range m.entityVariables[in.GetEntity().GetDcids()[0]] {
		resp.ByVariable[varDcid] = &pbv2.VariableObservation{}
	}
	return resp, nil
}

func (m *mockV2SearchIndicatorsServer) V2Node(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	m.mu.Lock()
	m.nodeCalls++
	m.mu.Unlock()
	if err := m.errs["V2Node"]; err != nil {
		return nil, err
	}
	resp := &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{}}
	for _, node := range in.GetNodes() {
		graph := &pbv2.LinkedGraph{}
		if types := m.entityTypes[node]; len(types) > 0 {
			var typeNodes []*pb.EntityInfo
			for _, t := range types {
				typeNodes = append(typeNodes, &pb.EntityInfo{Dcid: t})
			}
			graph.Arcs = map[string]*pbv2.Nodes{"typeOf": {Nodes: typeNodes}}
		}
		resp.Data[node] = graph
	}
	return resp, nil
}

// candidate returns the resolver candidate for a topic or variable DCID, with the topic's direct members as
// children when withMembers is set.
func (m *mockV2SearchIndicatorsServer) candidate(dcid string, withMembers bool) *pbv2.ResolveResponse_Entity_Candidate {
	topic, ok := m.topics[dcid]
	if !ok {
		return &pbv2.ResolveResponse_Entity_Candidate{Dcid: dcid, Name: m.variableNames[dcid], TypeOf: []string{DcidTypeVariable}}
	}
	c := &pbv2.ResolveResponse_Entity_Candidate{Dcid: dcid, Name: topic.name, TypeOf: []string{DcidTypeTopic}}
	if withMembers {
		for _, member := range topic.members {
			c.Children = append(c.Children, m.candidate(member, false))
		}
	}
	return c
}

// descendants returns the variables at any depth under a topic, repeating variables listed under several
// sub-topics like the real topic expander.
func (m *mockV2SearchIndicatorsServer) descendants(topicDcid string) []string {
	var vars []string
	for _, member := range m.topics[topicDcid].members {
		if _, ok := m.topics[member]; ok {
			vars = append(vars, m.descendants(member)...)
		} else {
			vars = append(vars, member)
		}
	}
	return vars
}

// newSearchIndicatorsMock returns a mock with a small health and economy topic tree.
func newSearchIndicatorsMock() *mockV2SearchIndicatorsServer {
	return &mockV2SearchIndicatorsServer{
		topics: map[string]mockTopic{
			"dc/topic/Health":    {name: "Health", members: []string{"dc/topic/Insurance", "Count_Uninsured", "Count_Asthma"}},
			"dc/topic/Insurance": {name: "Health Insurance", members: []string{"Count_Uninsured", "Count_Insured"}},
			"dc/topic/Economy":   {name: "Economy", members: []string{"UnemploymentRate", "dc/topic/Empty"}},
			"dc/topic/Empty":     {name: "Topic Without Variables"},
		},
		rootTopics: []string{"dc/topic/Health", "dc/topic/Economy"},
		variableNames: map[string]string{
			"Count_Uninsured":  "People Without Health Insurance",
			"Count_Insured":    "People With Health Insurance",
			"Count_Asthma":     "People With Asthma",
			"UnemploymentRate": "Unemployment Rate",
			"Count_Ungrouped":  "A Variable Not In Any Topic",
		},
		searchResults: map[string][]string{
			"health": {"dc/topic/Health", "dc/topic/Empty", "Count_Asthma", "Count_Uninsured", "dc/topic/Economy", "Count_Asthma"},
			"asthma": {"Count_Asthma"},
		},
		entityVariables: map[string][]string{
			"geoId/06":    {"Count_Uninsured", "UnemploymentRate"},
			"country/KEN": {"UnemploymentRate"},
			"country/FRA": {"Count_Ungrouped"},
			"country/IND": {"Count_Asthma"},
		},
		entityTypes: map[string][]string{
			"geoId/06":    {"State"},
			"country/KEN": {"Country"},
			"country/FRA": {"Country"},
			"country/IND": {"Country"},
			"dc/school/1": {"School"},
		},
	}
}

// Column lists of the v2 search_indicators tables.
var (
	v2TopicColumns    = []string{"dcid", "name", "entities_with_data", "member_topics", "member_variables", "descendant_variable_count"}
	v2VariableColumns = []string{"dcid", "name", "entities_with_data", "observation_properties"}
)

// refs builds member lists from alternating DCIDs and names.
func refs(dcidsAndNames ...string) []v2IndicatorRef {
	out := []v2IndicatorRef{}
	for i := 0; i < len(dcidsAndNames); i += 2 {
		out = append(out, v2IndicatorRef{Dcid: dcidsAndNames[i], Name: dcidsAndNames[i+1]})
	}
	return out
}

func TestV2SearchIndicators(t *testing.T) {
	tests := []struct {
		name          string
		req           *V2SearchIndicatorsRequest
		want          *V2SearchIndicatorsResponse
		wantNodeCalls int
	}{
		{
			name: "search with entities keeps rows and members with data, in rank order",
			req:  &V2SearchIndicatorsRequest{Query: "health", EntityDcids: []string{"geoId/06"}},
			want: &V2SearchIndicatorsResponse{
				TopicCandidates: &Table{Columns: v2TopicColumns, Rows: [][]any{
					{"dc/topic/Health", "Health", []string{"geoId/06"},
						refs("dc/topic/Insurance", "Health Insurance"),
						refs("Count_Uninsured", "People Without Health Insurance"), 1},
					{"dc/topic/Economy", "Economy", []string{"geoId/06"},
						refs(), refs("UnemploymentRate", "Unemployment Rate"), 1},
				}},
				VariableCandidates: &Table{Columns: v2VariableColumns, Rows: [][]any{
					{"Count_Uninsured", "People Without Health Insurance", []string{"geoId/06"}, []string{}},
				}},
			},
		},
		{
			name: "search without entities returns unfiltered rows, counting distinct descendants and dropping empty topics",
			req:  &V2SearchIndicatorsRequest{Query: "  health  "},
			want: &V2SearchIndicatorsResponse{
				TopicCandidates: &Table{Columns: v2TopicColumns, Rows: [][]any{
					{"dc/topic/Health", "Health", []string{},
						refs("dc/topic/Insurance", "Health Insurance"),
						refs("Count_Uninsured", "People Without Health Insurance", "Count_Asthma", "People With Asthma"), 3},
					{"dc/topic/Economy", "Economy", []string{},
						refs(), refs("UnemploymentRate", "Unemployment Rate"), 1},
				}},
				VariableCandidates: &Table{Columns: v2VariableColumns, Rows: [][]any{
					{"Count_Asthma", "People With Asthma", []string{}, []string{}},
					{"Count_Uninsured", "People Without Health Insurance", []string{}, []string{}},
				}},
				Note: v2NoEntitiesNote,
			},
		},
		{
			name: "browse requested topics in request order, without a variables table",
			req:  &V2SearchIndicatorsRequest{TopicDcids: []string{"dc/topic/Insurance", "dc/topic/Health", "dc/topic/Insurance"}, EntityDcids: []string{"geoId/06", "country/KEN"}},
			want: &V2SearchIndicatorsResponse{
				TopicCandidates: &Table{Columns: v2TopicColumns, Rows: [][]any{
					{"dc/topic/Insurance", "Health Insurance", []string{"geoId/06"},
						refs(), refs("Count_Uninsured", "People Without Health Insurance"), 1},
					{"dc/topic/Health", "Health", []string{"geoId/06"},
						refs("dc/topic/Insurance", "Health Insurance"),
						refs("Count_Uninsured", "People Without Health Insurance"), 1},
				}},
			},
		},
		{
			name: "browse drops member sub-topics without data for the entities",
			req:  &V2SearchIndicatorsRequest{TopicDcids: []string{"dc/topic/Health"}, EntityDcids: []string{"country/IND"}},
			want: &V2SearchIndicatorsResponse{
				TopicCandidates: &Table{Columns: v2TopicColumns, Rows: [][]any{
					{"dc/topic/Health", "Health", []string{"country/IND"},
						refs(), refs("Count_Asthma", "People With Asthma"), 1},
				}},
			},
		},
		{
			name: "browse drops requested topics without data",
			req:  &V2SearchIndicatorsRequest{TopicDcids: []string{"dc/topic/Health", "dc/topic/Economy"}, EntityDcids: []string{"country/KEN"}},
			want: &V2SearchIndicatorsResponse{
				TopicCandidates: &Table{Columns: v2TopicColumns, Rows: [][]any{
					{"dc/topic/Economy", "Economy", []string{"country/KEN"},
						refs(), refs("UnemploymentRate", "Unemployment Rate"), 1},
				}},
			},
		},
		{
			name: "browse top-level topics when topic_dcids are blank",
			req:  &V2SearchIndicatorsRequest{TopicDcids: []string{" ", ""}},
			want: &V2SearchIndicatorsResponse{
				TopicCandidates: &Table{Columns: v2TopicColumns, Rows: [][]any{
					{"dc/topic/Health", "Health", []string{},
						refs("dc/topic/Insurance", "Health Insurance"),
						refs("Count_Uninsured", "People Without Health Insurance", "Count_Asthma", "People With Asthma"), 3},
					{"dc/topic/Economy", "Economy", []string{},
						refs(), refs("UnemploymentRate", "Unemployment Rate"), 1},
				}},
				Note: v2NoEntitiesNote,
			},
		},
		{
			name: "entity without any data gets the no-data note after an existence check",
			req:  &V2SearchIndicatorsRequest{Query: "health", EntityDcids: []string{"dc/school/1"}},
			want: &V2SearchIndicatorsResponse{
				TopicCandidates:    &Table{Columns: v2TopicColumns},
				VariableCandidates: &Table{Columns: v2VariableColumns},
				Note:               v2NoEntityDataNote,
			},
			wantNodeCalls: 1,
		},
		{
			name: "entity without any data gets the no-data note even when nothing matches",
			req:  &V2SearchIndicatorsRequest{Query: "zzz", EntityDcids: []string{"dc/school/1"}},
			want: &V2SearchIndicatorsResponse{
				TopicCandidates:    &Table{Columns: v2TopicColumns},
				VariableCandidates: &Table{Columns: v2VariableColumns},
				Note:               v2NoEntityDataNote,
			},
			wantNodeCalls: 1,
		},
		{
			name: "search without matches gets the no-match note",
			req:  &V2SearchIndicatorsRequest{Query: "zzz", EntityDcids: []string{"geoId/06"}},
			want: &V2SearchIndicatorsResponse{
				TopicCandidates:    &Table{Columns: v2TopicColumns},
				VariableCandidates: &Table{Columns: v2VariableColumns},
				Note:               v2NoMatchNote,
			},
		},
		{
			name: "search matches without data get the no-query-data note",
			req:  &V2SearchIndicatorsRequest{Query: "asthma", EntityDcids: []string{"geoId/06"}},
			want: &V2SearchIndicatorsResponse{
				TopicCandidates:    &Table{Columns: v2TopicColumns},
				VariableCandidates: &Table{Columns: v2VariableColumns},
				Note:               v2NoQueryDataNote,
			},
		},
		{
			name: "requested topics without data get the requested-topics note",
			req:  &V2SearchIndicatorsRequest{TopicDcids: []string{"dc/topic/Health"}, EntityDcids: []string{"country/KEN"}},
			want: &V2SearchIndicatorsResponse{
				TopicCandidates: &Table{Columns: v2TopicColumns},
				Note:            v2NoRequestedTopicDataNote,
			},
		},
		{
			name: "top-level topics without data get the top-level note",
			req:  &V2SearchIndicatorsRequest{EntityDcids: []string{"country/FRA"}},
			want: &V2SearchIndicatorsResponse{
				TopicCandidates: &Table{Columns: v2TopicColumns},
				Note:            v2NoTopLevelTopicDataNote,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := newSearchIndicatorsMock()
			got, err := NewService(mock, NewCache(mock), nil).V2SearchIndicators(context.Background(), tc.req)
			if err != nil {
				t.Fatalf("V2SearchIndicators() error = %v", err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("V2SearchIndicators() mismatch (-want +got):\n%s", diff)
			}
			if mock.nodeCalls != tc.wantNodeCalls {
				t.Errorf("V2Node calls = %d, want %d", mock.nodeCalls, tc.wantNodeCalls)
			}
		})
	}
}

func TestV2SearchIndicatorsSearchCaps(t *testing.T) {
	tests := []struct {
		name          string
		entityDcids   []string
		wantTopics    []string
		wantVariables []string
	}{
		{
			name:          "without entities, the first 10 topics and variables in rank order",
			wantTopics:    []string{"dc/topic/T0", "dc/topic/T1", "dc/topic/T2", "dc/topic/T3", "dc/topic/T4", "dc/topic/T5", "dc/topic/T6", "dc/topic/T7", "dc/topic/T8", "dc/topic/T9"},
			wantVariables: []string{"Var0", "Var1", "Var2", "Var3", "Var4", "Var5", "Var6", "Var7", "Var8", "Var9"},
		},
		{
			name:          "with entities, rows without data are skipped and later candidates fill the tables",
			entityDcids:   []string{"geoId/01"},
			wantTopics:    []string{"dc/topic/T2", "dc/topic/T3", "dc/topic/T4", "dc/topic/T5", "dc/topic/T6", "dc/topic/T7", "dc/topic/T8", "dc/topic/T9", "dc/topic/T10", "dc/topic/T11"},
			wantVariables: []string{"Var2", "Var3", "Var4", "Var5", "Var6", "Var7", "Var8", "Var9", "Var10", "Var11"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := newSearchIndicatorsMock()
			var results []string
			for i := range 12 {
				topic := fmt.Sprintf("dc/topic/T%d", i)
				variable := fmt.Sprintf("Var%d", i)
				mock.topics[topic] = mockTopic{name: topic, members: []string{variable}}
				mock.variableNames[variable] = variable
				results = append(results, topic, variable)
				if i >= 2 {
					mock.entityVariables["geoId/01"] = append(mock.entityVariables["geoId/01"], variable)
				}
			}
			mock.searchResults["many"] = results

			got, err := NewService(mock, NewCache(mock), nil).V2SearchIndicators(context.Background(),
				&V2SearchIndicatorsRequest{Query: "many", EntityDcids: tc.entityDcids})
			if err != nil {
				t.Fatalf("V2SearchIndicators() error = %v", err)
			}
			if diff := cmp.Diff(tc.wantTopics, firstColumn(got.TopicCandidates)); diff != "" {
				t.Errorf("topic rows mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantVariables, firstColumn(got.VariableCandidates)); diff != "" {
				t.Errorf("variable rows mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// firstColumn returns the first column (dcid) of every row.
func firstColumn(table *Table) []string {
	var dcids []string
	for _, row := range table.Rows {
		dcids = append(dcids, row[0].(string))
	}
	return dcids
}

func TestV2SearchIndicatorsBrowseKeepsRequestOrder(t *testing.T) {
	mock := newSearchIndicatorsMock()
	mock.reverseTopicEntities = true
	got, err := NewService(mock, NewCache(mock), nil).V2SearchIndicators(context.Background(),
		&V2SearchIndicatorsRequest{TopicDcids: []string{"dc/topic/Insurance", "dc/topic/Economy", "dc/topic/Health"}})
	if err != nil {
		t.Fatalf("V2SearchIndicators() error = %v", err)
	}
	want := []string{"dc/topic/Insurance", "dc/topic/Economy", "dc/topic/Health"}
	if diff := cmp.Diff(want, firstColumn(got.TopicCandidates)); diff != "" {
		t.Errorf("topic rows mismatch (-want +got):\n%s", diff)
	}
}

func TestV2SearchIndicatorsErrors(t *testing.T) {
	manyDcids := func(prefix string, n int) []string {
		var dcids []string
		for i := range n {
			dcids = append(dcids, fmt.Sprintf("%s%d", prefix, i))
		}
		return dcids
	}
	tests := []struct {
		name     string
		req      *V2SearchIndicatorsRequest
		errs     map[string]error
		wantCode codes.Code
		wantMsg  string
	}{
		{
			name:     "query and topic_dcids",
			req:      &V2SearchIndicatorsRequest{Query: "health", TopicDcids: []string{"dc/topic/Health"}},
			wantCode: codes.InvalidArgument,
			wantMsg:  "pass either 'query' (to search) or 'topic_dcids' (to browse those topics), not both",
		},
		{
			name:     "too many entity_dcids",
			req:      &V2SearchIndicatorsRequest{Query: "health", EntityDcids: manyDcids("geoId/", 21)},
			wantCode: codes.InvalidArgument,
			wantMsg:  "too many entity_dcids (21): pass at most 20 per call and split the rest into additional calls",
		},
		{
			name:     "too many topic_dcids",
			req:      &V2SearchIndicatorsRequest{TopicDcids: manyDcids("dc/topic/T", 11)},
			wantCode: codes.InvalidArgument,
			wantMsg:  "too many topic_dcids (11): pass at most 10 per call",
		},
		{
			name:     "invalid target",
			req:      &V2SearchIndicatorsRequest{Query: "health", Target: "everything"},
			wantCode: codes.InvalidArgument,
			wantMsg:  `invalid target "everything", must be one of [base_and_custom base_only custom_only]`,
		},
		{
			name:     "unknown entity DCIDs and names, but not known entities without data",
			req:      &V2SearchIndicatorsRequest{Query: "health", EntityDcids: []string{"geoId/06", "bogus/1", "dc/school/1", "California"}},
			wantCode: codes.InvalidArgument,
			wantMsg:  `unknown entity_dcids ["bogus/1" "California"]: pass DCIDs returned by resolve_entities (e.g. 'geoId/06'), not names`,
		},
		{
			name:     "unknown topics and variables passed as topics",
			req:      &V2SearchIndicatorsRequest{TopicDcids: []string{"dc/topic/Health", "dc/topic/Nope", "Count_Asthma"}},
			wantCode: codes.InvalidArgument,
			wantMsg:  `unknown topic_dcids ["dc/topic/Nope" "Count_Asthma"]: pass topic DCIDs from search_indicators results (a topic's dcid or its member_topics)`,
		},
		{
			name:     "search backend not available in this deployment",
			req:      &V2SearchIndicatorsRequest{Query: "health"},
			errs:     map[string]error{ResolverIndicator: status.Error(codes.FailedPrecondition, "Embeddings service client is not initialized.")},
			wantCode: codes.FailedPrecondition,
			wantMsg:  "failed to search indicators: this operation is not available in this deployment; do not retry it",
		},
		{
			name:     "topic backend failure while browsing",
			req:      &V2SearchIndicatorsRequest{TopicDcids: []string{"dc/topic/Health"}},
			errs:     map[string]error{ResolverTopic: status.Error(codes.Unavailable, "topic cache at 10.0.0.1:6379 unreachable")},
			wantCode: codes.Unavailable,
			wantMsg:  "failed to look up topics: a backend service is temporarily unavailable; retry the call shortly",
		},
		{
			name:     "availability timeout",
			req:      &V2SearchIndicatorsRequest{Query: "health", EntityDcids: []string{"geoId/06"}},
			errs:     map[string]error{"V2Observation": context.DeadlineExceeded},
			wantCode: codes.DeadlineExceeded,
			wantMsg:  "failed to check which indicators have data for entity_dcids: a backend service timed out; retry the call shortly",
		},
		{
			name:     "entity lookup failure",
			req:      &V2SearchIndicatorsRequest{Query: "health", EntityDcids: []string{"bogus/1"}},
			errs:     map[string]error{"V2Node": status.Error(codes.Internal, "spanner: session projects/p/instances/i/databases/d/sessions/s")},
			wantCode: codes.Internal,
			wantMsg:  "failed to look up entity_dcids: a backend service is temporarily unavailable; retry the call shortly",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := newSearchIndicatorsMock()
			mock.errs = tc.errs
			_, err := NewService(mock, NewCache(mock), nil).V2SearchIndicators(context.Background(), tc.req)
			st := status.Convert(err)
			if st.Code() != tc.wantCode || st.Message() != tc.wantMsg {
				t.Errorf("V2SearchIndicators() error = (%v, %q), want (%v, %q)", st.Code(), st.Message(), tc.wantCode, tc.wantMsg)
			}
			for _, backendErr := range tc.errs {
				if detail := status.Convert(backendErr).Message(); strings.Contains(st.Message(), detail) {
					t.Errorf("error message %q leaks backend detail %q", st.Message(), detail)
				}
			}
		})
	}
}

func TestNormalizeV2SearchIndicatorsRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *V2SearchIndicatorsRequest
		want *V2SearchIndicatorsRequest
	}{
		{
			name: "nil browses the top-level topics",
			want: &V2SearchIndicatorsRequest{},
		},
		{
			name: "trims, drops blanks and duplicates, keeps order",
			req: &V2SearchIndicatorsRequest{
				Query:       " health ",
				TopicDcids:  []string{" ", ""},
				EntityDcids: []string{"geoId/06", " country/KEN ", "", "geoId/06"},
				Target:      "base_only",
			},
			want: &V2SearchIndicatorsRequest{
				Query:       "health",
				EntityDcids: []string{"geoId/06", "country/KEN"},
				Target:      "base_only",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if diff := cmp.Diff(tc.want, normalizeV2SearchIndicatorsRequest(tc.req)); diff != "" {
				t.Errorf("normalizeV2SearchIndicatorsRequest() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
