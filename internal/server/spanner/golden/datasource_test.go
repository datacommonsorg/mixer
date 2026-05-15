// Copyright 2024 Google LLC
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

package golden

import (
	"context"
	"path"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/datacommonsorg/mixer/internal/maps"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
	"github.com/datacommonsorg/mixer/internal/server/spanner"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/store/files"
	"github.com/datacommonsorg/mixer/internal/translator/types"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

type mockSpannerClient struct {
	resolveByIDRes                     map[string][]string
	getNodeEdgesRes                    map[string][]*spanner.Edge
	checkVariableExistenceRes          [][]string
	filterNodesByTypeRes               map[string][]string
	getObservationsRes                 []*spanner.Observation
	getObservationsContainedInPlaceRes []*spanner.Observation
}

func (m *mockSpannerClient) GetNodeProps(ctx context.Context, ids []string, out bool) (map[string][]*spanner.Property, error) {
	return nil, nil
}
func (m *mockSpannerClient) GetNodeEdgesByID(ctx context.Context, ids []string, arc *v2.Arc, pageSize, offset int) (map[string][]*spanner.Edge, error) {
	return m.getNodeEdgesRes, nil
}
func (m *mockSpannerClient) GetObservations(ctx context.Context, variables []string, entities []string) ([]*spanner.Observation, error) {
	return m.getObservationsRes, nil
}
func (m *mockSpannerClient) CheckVariableExistence(ctx context.Context, variables []string, entities []string) ([][]string, error) {
	return m.checkVariableExistenceRes, nil
}
func (m *mockSpannerClient) GetObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace) ([]*spanner.Observation, error) {
	return m.getObservationsContainedInPlaceRes, nil
}
func (m *mockSpannerClient) GetSdmxObservations(ctx context.Context, req *pb.SdmxDataQuery) (*pb.SdmxDataResult, error) {
	return nil, nil
}
func (m *mockSpannerClient) SearchNodes(ctx context.Context, query string, types []string) ([]*spanner.SearchNode, error) {
	return nil, nil
}
func (m *mockSpannerClient) ResolveByID(ctx context.Context, nodes []string, in, out string) (map[string][]string, error) {
	// ResolveByID mocks the Spanner lookup of placeId to DCID.
	// Maps-specific fallbacks are not required for these unit tests.
	return m.resolveByIDRes, nil
}
func (m *mockSpannerClient) Sparql(ctx context.Context, nodes []types.Node, queries []*types.Query, opts *types.QueryOptions) ([][]string, error) {
	return nil, nil
}
func (m *mockSpannerClient) GetProvenanceSummary(ctx context.Context, ids []string) (map[string]map[string]*pb.StatVarSummary_ProvenanceSummary, error) {
	return nil, nil
}
func (m *mockSpannerClient) GetTermEmbeddingQuery(ctx context.Context, modelName, searchLabel, taskType string) ([]float64, error) {
	return nil, nil
}
func (m *mockSpannerClient) FilterNodesByTypes(ctx context.Context, nodes []string, typeFilters []string) (map[string][]string, error) {
	res := map[string][]string{}
	for _, typeFilter := range typeFilters {
		allowedNodes := m.filterNodesByTypeRes[typeFilter]
		for _, node := range nodes {
			if slices.Contains(allowedNodes, node) {
				res[node] = append(res[node], typeFilter)
			}
		}
	}
	return res, nil
}
func (m *mockSpannerClient) VectorSearchQuery(ctx context.Context, tableName string, limit int, embeddings []float64, numLeaves int, threshold float64, nodeTypes []string) ([]*spanner.VectorSearchResult, error) {
	return nil, nil
}
func (m *mockSpannerClient) GetEventCollectionDate(ctx context.Context, placeID, eventType string) ([]string, error) {
	return nil, nil
}
func (m *mockSpannerClient) GetEventCollectionDcids(ctx context.Context, placeID, eventType, date string) ([]spanner.EventIdWithMagnitudeDcid, error) {
	return nil, nil
}
func (m *mockSpannerClient) GetEventCollection(ctx context.Context, req *pbv1.EventCollectionRequest) (*pbv1.EventCollection, error) {
	return nil, nil
}
func (m *mockSpannerClient) GetStatVarGroupNode(ctx context.Context, nodes []string, includeDefinitions bool) ([]*spanner.StatVarGroupNode, error) {
	return nil, nil
}
func (m *mockSpannerClient) GetFilteredStatVarGroupNode(ctx context.Context, nodes []string, constrainedPlaces []string, constrainedImport string, numEntitiesExistence int, includeDefinitions bool) (map[string]*spanner.FilteredStatVarGroupNode, error) {
	return nil, nil
}
func (m *mockSpannerClient) GetFilteredTopic(ctx context.Context, nodes []string, constrainedPlaces []string, constrainedImport string, numEntitiesExistence int) (map[string]int, error) {
	return nil, nil
}
func (m *mockSpannerClient) Id() string { return "mock" }
func (m *mockSpannerClient) Start()     {}
func (m *mockSpannerClient) Close()     {}

func TestSpannerResolve(t *testing.T) {
	client := test.NewSpannerClient()
	if client == nil {
		client = &mockSpannerClient{
			getNodeEdgesRes: map[string][]*spanner.Edge{
				"geoId/06": {{SubjectID: "geoId/06", Value: "State"}},
			},
		}
	}
	// Minimal RecogPlaceStore for testing
	recogPlaceStore := &files.RecogPlaceStore{
		DcidToNames: map[string][]string{
			"geoId/06": {"California"},
		},
		RecogPlaceMap: map[string]*pb.RecogPlaces{
			"california": {
				Places: []*pb.RecogPlace{
					{
						Dcid: "geoId/06",
						Names: []*pb.RecogPlace_Name{
							{Parts: []string{"california"}},
						},
					},
				},
			},
		},
	}
	ds := spanner.NewSpannerDataSource(client, recogPlaceStore, &maps.FakeMapsClient{}, false)

	t.Parallel()
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "datasource")

	for _, c := range []struct {
		req        *pbv2.ResolveRequest
		goldenFile string
	}{
		{
			req: &pbv2.ResolveRequest{
				Nodes:    []string{"California"},
				Property: "<-description->dcid",
			},
			goldenFile: "resolve_description.json",
		},
		{
			req: &pbv2.ResolveRequest{
				Nodes:    []string{"California"},
				Property: "<-description{typeOf:State}->dcid",
			},
			goldenFile: "resolve_description_type_filter.json",
		},
	} {
		got, err := ds.Resolve(ctx, c.req)
		if err != nil {
			t.Fatalf("Resolve error (%v): %v", c.goldenFile, err)
		}

		if test.GenerateGolden {
			test.UpdateProtoGolden(got, goldenDir, c.goldenFile)
			continue
		}

		var want pbv2.ResolveResponse
		if err = test.ReadJSON(goldenDir, c.goldenFile, &want); err != nil {
			t.Fatalf("ReadJSON error (%v): %v", c.goldenFile, err)
		}

		cmpOpts := cmp.Options{
			protocmp.Transform(),
		}
		if diff := cmp.Diff(got, &want, cmpOpts); diff != "" {
			t.Errorf("%v payload mismatch:\n%v", c.goldenFile, diff)
		}
	}
}

func TestSpannerNode(t *testing.T) {
	client := test.NewSpannerClient()
	if client == nil {
		return
	}
	ds := spanner.NewSpannerDataSource(client, nil, nil, false)

	t.Parallel()
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "datasource")

	for _, c := range []struct {
		req        *pbv2.NodeRequest
		goldenFile string
	}{
		{
			req: &pbv2.NodeRequest{
				Nodes:    []string{"Person", "Count_Person"},
				Property: "->",
			},
			goldenFile: "node_properties.json",
		},
		{
			req: &pbv2.NodeRequest{
				Nodes:    []string{"Monthly_Average_RetailPrice_Electricity_Residential", "Aadhaar", "foo"},
				Property: "->[typeOf, name, statType]",
			},
			goldenFile: "node_property_values.json",
		},
	} {
		got, err := ds.Node(ctx, c.req, datasources.DefaultPageSize)
		if err != nil {
			t.Fatalf("Node error (%v): %v", c.goldenFile, err)
		}

		if test.GenerateGolden {
			test.UpdateProtoGolden(got, goldenDir, c.goldenFile)
			return
		}

		var want pbv2.NodeResponse
		if err = test.ReadJSON(goldenDir, c.goldenFile, &want); err != nil {
			t.Fatalf("ReadJSON error (%v): %v", c.goldenFile, err)
		}

		cmpOpts := cmp.Options{
			protocmp.Transform(),
		}
		if diff := cmp.Diff(got, &want, cmpOpts); diff != "" {
			t.Errorf("%v payload mismatch:\n%v", c.goldenFile, diff)
		}
	}
}

func TestSpannerSparql(t *testing.T) {
	client := test.NewSpannerClient()
	if client == nil {
		return
	}
	ds := spanner.NewSpannerDataSource(client, nil, nil, false)

	t.Parallel()
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "datasource")

	for _, c := range []struct {
		req        *pb.SparqlRequest
		goldenFile string
	}{
		{
			req: &pb.SparqlRequest{
				Query: `SELECT ?name
		                WHERE {
		                  ?country typeOf Country .
		                  ?country name ?name .
		                }
		                ORDER BY DESC(?name)
		                LIMIT 10
						`,
			},
			goldenFile: "sparql_country_names_desc.json",
		},
		{
			req: &pb.SparqlRequest{
				Query: `SELECT ?dcid ?name
                WHERE {
                  ?state typeOf State .
				  ?state dcid geoId/10 .
                  ?dcid containedInPlace ?state .
				  ?dcid typeOf County .
				  ?dcid name ?name .
                }
                ORDER BY ASC(?dcid)
				`,
			},
			goldenFile: "sparql_delaware_counties.json",
		},
	} {
		got, err := ds.Sparql(ctx, c.req)
		if err != nil {
			t.Fatalf("Sparql error (%v): %v", c.goldenFile, err)
		}

		if test.GenerateGolden {
			test.UpdateProtoGolden(got, goldenDir, c.goldenFile)
			return
		}

		var want pb.QueryResponse
		if err = test.ReadJSON(goldenDir, c.goldenFile, &want); err != nil {
			t.Fatalf("ReadJSON error (%v): %v", c.goldenFile, err)
		}

		cmpOpts := cmp.Options{
			protocmp.Transform(),
		}
		if diff := cmp.Diff(got, &want, cmpOpts); diff != "" {
			t.Errorf("%v payload mismatch:\n%v", c.goldenFile, diff)
		}
	}
}

func TestSpannerEvent(t *testing.T) {
	client := test.NewSpannerClient()
	if client == nil {
		return
	}
	ds := spanner.NewSpannerDataSource(client, nil, nil, false)

	t.Parallel()
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "datasource")

	for _, c := range []struct {
		req        *pbv2.EventRequest
		goldenFile string
	}{
		{
			req: &pbv2.EventRequest{
				Node:     "country/LBR",
				Property: "<-location{typeOf:FireEvent}->date",
			},
			goldenFile: "event_collection_date_lbr.json",
		},
		{
			req: &pbv2.EventRequest{
				Node:     "country/LBR",
				Property: "<-location{typeOf:FireEvent,date:2020-10}",
			},
			goldenFile: "event_collection_lbr.json",
		},
		{
			req: &pbv2.EventRequest{
				Node:     "country/LBR",
				Property: "<-location{typeOf:FireEvent,date:2020-10,area:100#200#SquareKilometer}",
			},
			goldenFile: "event_collection_lbr_filtered.json",
		},
	} {
		got, err := ds.Event(ctx, c.req)
		if err != nil {
			t.Fatalf("Event error (%v): %v", c.goldenFile, err)
		}
		// Trim to 10 events to avoid very large golden files.
		if got.EventCollection != nil && len(got.EventCollection.Events) > 10 {
			got.EventCollection.Events = got.EventCollection.Events[:10]
		}

		if test.GenerateGolden {
			test.UpdateProtoGolden(got, goldenDir, c.goldenFile)
			continue
		}

		var want pbv2.EventResponse
		if err = test.ReadJSON(goldenDir, c.goldenFile, &want); err != nil {
			t.Fatalf("ReadJSON error (%v): %v", c.goldenFile, err)
		}

		cmpOpts := cmp.Options{
			protocmp.Transform(),
		}
		if diff := cmp.Diff(got, &want, cmpOpts); diff != "" {
			t.Errorf("%v payload mismatch:\n%v", c.goldenFile, diff)
		}
	}
}

func TestSpannerObservation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "datasource")

	for _, c := range []struct {
		desc       string
		req        *pbv2.ObservationRequest
		mockRes    [][]string
		goldenFile string
		wantErr    bool
	}{
		{
			desc: "Basic existence check (single entity)",
			req: &pbv2.ObservationRequest{
				Variable: &pbv2.DcidOrExpression{
					Dcids: []string{"Count_Person", "Median_Income_Person", "NonExistent"},
				},
				Entity: &pbv2.DcidOrExpression{
					Dcids: []string{"geoId/06"},
				},
				Select: []string{"variable", "entity"},
			},
			mockRes: [][]string{
				{"Count_Person", "geoId/06"},
				{"Median_Income_Person", "geoId/06"},
			},
			goldenFile: "observation_existence.json",
		},
		{
			desc: "Multi-entity existence check",
			req: &pbv2.ObservationRequest{
				Variable: &pbv2.DcidOrExpression{
					Dcids: []string{"Count_Person"},
				},
				Entity: &pbv2.DcidOrExpression{
					Dcids: []string{"geoId/06", "geoId/01"},
				},
				Select: []string{"variable", "entity"},
			},
			mockRes: [][]string{
				{"Count_Person", "geoId/06"},
				{"Count_Person", "geoId/01"},
			},
			goldenFile: "observation_existence_multi_entity.json",
		},
		{
			desc: "No variables requested (returns all vars for entity)",
			req: &pbv2.ObservationRequest{
				Variable: &pbv2.DcidOrExpression{
					Dcids: []string{},
				},
				Entity: &pbv2.DcidOrExpression{
					Dcids: []string{"geoId/06"},
				},
				Select: []string{"variable", "entity"},
			},
			mockRes: [][]string{
				{"Count_Person", "geoId/06"},
				{"Median_Income_Person", "geoId/06"},
			},
			goldenFile: "observation_existence_no_vars.json",
		},
		{
			desc: "Single entity existence check",
			req: &pbv2.ObservationRequest{
				Variable: &pbv2.DcidOrExpression{
					Dcids: []string{"Count_Person"},
				},
				Entity: &pbv2.DcidOrExpression{
					Dcids: []string{"geoId/06"},
				},
				Select: []string{"variable", "entity"},
			},
			mockRes: [][]string{
				{"Count_Person", "geoId/06"},
			},
			goldenFile: "observation_existence_single.json",
		},
		{
			desc: "No entities requested (error case)",
			req: &pbv2.ObservationRequest{
				Variable: &pbv2.DcidOrExpression{
					Dcids: []string{"Count_Person"},
				},
				Entity: &pbv2.DcidOrExpression{
					Dcids: []string{},
				},
				Select: []string{"variable", "entity"},
			},
			mockRes: [][]string{},
			wantErr: true,
		},
	} {
		client := &mockSpannerClient{
			checkVariableExistenceRes: c.mockRes,
		}
		ds := spanner.NewSpannerDataSource(client, nil, nil, false)

		got, err := ds.Observation(ctx, c.req)
		if (err != nil) != c.wantErr {
			t.Fatalf("%s: Observation error = %v, wantErr %v", c.desc, err, c.wantErr)
		}
		if c.wantErr {
			continue
		}

		if test.GenerateGolden {
			test.UpdateProtoGolden(got, goldenDir, c.goldenFile)
			continue
		}

		var want pbv2.ObservationResponse
		if err = test.ReadJSON(goldenDir, c.goldenFile, &want); err != nil {
			t.Fatalf("%s: ReadJSON error (%v): %v", c.desc, c.goldenFile, err)
		}

		cmpOpts := cmp.Options{
			protocmp.Transform(),
		}
		if diff := cmp.Diff(got, &want, cmpOpts); diff != "" {
			t.Errorf("%s: %v payload mismatch:\n%v", c.desc, c.goldenFile, diff)
		}
	}
}

func TestBulkVariableGroupInfo_Filtering(t *testing.T) {
	ctx := context.Background()

	// Setup mock to return specific nodes for specific types
	client := &mockSpannerClient{
		filterNodesByTypeRes: map[string][]string{
			"StatVarGroup": {"dc/g/Demographics", "WHO/Root"},
			"Topic":        {"dc/topic/Demographics"},
		},
	}
	ds := spanner.NewSpannerDataSource(client, nil, nil, false)

	// Test Case 1: Valid SVGs including WHO/Root
	req1 := &pbv1.BulkVariableGroupInfoRequest{
		Nodes: []string{"dc/g/Demographics", "WHO/Root"},
	}
	_, err := ds.BulkVariableGroupInfo(ctx, req1)
	// We expect no error here regarding node validation
	if err != nil && strings.Contains(err.Error(), "is not a valid StatVarGroup") {
		t.Errorf("Expected WHO/Root to be valid, got error: %v", err)
	}

	// Test Case 2: Invalid node (neither SVG nor Topic) should return empty result without duplication
	req2 := &pbv1.BulkVariableGroupInfoRequest{
		Nodes: []string{"InvalidNode", "InvalidNode"},
	}
	resp, err := ds.BulkVariableGroupInfo(ctx, req2)
	if err != nil {
		t.Errorf("Expected no error for InvalidNode, got: %v", err)
	}
	count := 0
	for _, data := range resp.GetData() {
		if data.Node == "InvalidNode" {
			count++
			if data.Info != nil && (data.Info.AbsoluteName != "" || len(data.Info.ChildStatVars) > 0) {
				t.Errorf("Expected empty result for InvalidNode, got: %v", data.Info)
			}
		}
	}
	if count != 1 {
		t.Errorf("Expected exactly one InvalidNode in response, got: %d", count)
	}

	// Test Case 3: Mixed nodes (Topics and SVGs)
	req3 := &pbv1.BulkVariableGroupInfoRequest{
		Nodes: []string{"dc/g/Demographics", "dc/topic/Demographics"},
	}
	_, err = ds.BulkVariableGroupInfo(ctx, req3)
	if err == nil || !strings.Contains(err.Error(), "cannot mix Topic and StatVarGroup nodes") {
		t.Errorf("Expected error for mixed nodes, got: %v", err)
	}

	// Test Case 4: Excluded node should return empty result
	req4 := &pbv1.BulkVariableGroupInfoRequest{
		Nodes: []string{"dc/g/Hidden"},
	}
	// Update mock to return it as a StatVarGroup
	client.filterNodesByTypeRes["StatVarGroup"] = append(client.filterNodesByTypeRes["StatVarGroup"], "dc/g/Hidden")

	resp4, err := ds.BulkVariableGroupInfo(ctx, req4)
	if err != nil {
		t.Errorf("Expected no error for ExcludedNode, got: %v", err)
	}
	found := false
	for _, data := range resp4.GetData() {
		if data.Node == "dc/g/Hidden" {
			found = true
			if data.Info != nil && (data.Info.AbsoluteName != "" || len(data.Info.ChildStatVars) > 0) {
				t.Errorf("Expected empty result for ExcludedNode, got: %v", data.Info)
			}
		}
	}
	if !found {
		t.Errorf("Expected ExcludedNode in response, but it was missing")
	}
}

// TODO: Move unit tests to a separate test file since this file is meant for golden tests.
func TestSpannerObservation_ExpressionExpansion(t *testing.T) {
	ctx := context.Background()

	// Mock Spanner client
	client := &mockSpannerClient{
		// Mock GetNodeEdgesByID to return local child places
		getNodeEdgesRes: map[string][]*spanner.Edge{
			"geoId/06": {
				{Value: "geoId/06002", Predicate: "linkedContainedInPlace"},
			},
		},
		// Mock GetObservations to return observations for merged list
		getObservationsRes: []*spanner.Observation{
			{
				VariableMeasured: "Count_Person",
				ObservationAbout: "geoId/06001", // Remote place
				Observations: []*spanner.DateValue{
					{Date: "2020", Value: "12345"},
				},
			},
			{
				VariableMeasured: "Count_Person",
				ObservationAbout: "geoId/06002", // Local place
				Observations: []*spanner.DateValue{
					{Date: "2020", Value: "67890"},
				},
			},
		},
	}

	ds := spanner.NewSpannerDataSource(client, nil, nil, false)

	// Test Case 1: Expression with Remote Data in Context
	req := &pbv2.ObservationRequest{
		Variable: &pbv2.DcidOrExpression{Dcids: []string{"Count_Person"}},
		Entity:   &pbv2.DcidOrExpression{Expression: "geoId/06<-containedInPlace+{typeOf:County}"},
		Select:   []string{"variable", "entity", "value"},
	}

	// Add remote DCIDs to context
	remoteDCIDs := []string{"geoId/06001"}
	ctxWithRemote := context.WithValue(ctx, dispatcher.RelationExpressionExpandedEntities, remoteDCIDs)

	resp, err := ds.Observation(ctxWithRemote, req)
	if err != nil {
		t.Fatalf("Observation failed: %v", err)
	}

	if resp == nil {
		t.Fatal("Expected non-nil response")
		return
	}

	// Verify that we have data for both geoId/06001 and geoId/06002
	byVariable := resp.ByVariable
	if byVariable == nil {
		t.Fatal("Expected ByVariable to be populated")
	}
	countPerson, ok := byVariable["Count_Person"]
	if !ok {
		t.Fatal("Expected Count_Person in response")
	}
	byEntity := countPerson.ByEntity
	if byEntity == nil {
		t.Fatal("Expected ByEntity to be populated")
	}

	if _, ok := byEntity["geoId/06001"]; !ok {
		t.Errorf("Expected data for geoId/06001 (remote place)")
	}
	if _, ok := byEntity["geoId/06002"]; !ok {
		t.Errorf("Expected data for geoId/06002 (local place)")
	}
}

func TestSpannerObservation_ExpressionExpansion_Fallback(t *testing.T) {
	ctx := context.Background()

	// Mock Spanner client
	client := &mockSpannerClient{
		// Mock GetObservationsContainedInPlace to return observations
		getObservationsContainedInPlaceRes: []*spanner.Observation{
			{
				VariableMeasured: "Count_Person",
				ObservationAbout: "geoId/06002", // Local place
				Observations: []*spanner.DateValue{
					{Date: "2020", Value: "67890"},
				},
			},
		},
	}

	ds := spanner.NewSpannerDataSource(client, nil, nil, false)

	req := &pbv2.ObservationRequest{
		Variable: &pbv2.DcidOrExpression{Dcids: []string{"Count_Person"}},
		Entity:   &pbv2.DcidOrExpression{Expression: "geoId/06<-containedInPlace+{typeOf:County}"},
		Select:   []string{"variable", "entity", "value"},
	}

	resp, err := ds.Observation(ctx, req)
	if err != nil {
		t.Fatalf("Observation failed: %v", err)
	}

	if resp == nil {
		t.Fatal("Expected non-nil response")
		return
	}

	byVariable := resp.ByVariable
	countPerson := byVariable["Count_Person"]
	byEntity := countPerson.ByEntity

	if _, ok := byEntity["geoId/06002"]; !ok {
		t.Errorf("Expected data for geoId/06002 (local place)")
	}
}

func TestSpannerObservation_NoExpression(t *testing.T) {
	ctx := context.Background()

	// Mock Spanner client
	client := &mockSpannerClient{
		// Mock GetObservations to return observations
		getObservationsRes: []*spanner.Observation{
			{
				VariableMeasured: "Count_Person",
				ObservationAbout: "geoId/06",
				Observations: []*spanner.DateValue{
					{Date: "2020", Value: "12345"},
				},
			},
		},
	}

	ds := spanner.NewSpannerDataSource(client, nil, nil, false)

	req := &pbv2.ObservationRequest{
		Variable: &pbv2.DcidOrExpression{Dcids: []string{"Count_Person"}},
		Entity:   &pbv2.DcidOrExpression{Dcids: []string{"geoId/06"}},
		Select:   []string{"variable", "entity", "value"},
	}

	resp, err := ds.Observation(ctx, req)
	if err != nil {
		t.Fatalf("Observation failed: %v", err)
	}

	if resp == nil {
		t.Fatal("Expected non-nil response")
		return
	}

	byVariable := resp.ByVariable
	countPerson := byVariable["Count_Person"]
	byEntity := countPerson.ByEntity

	if _, ok := byEntity["geoId/06"]; !ok {
		t.Errorf("Expected data for geoId/06")
	}
}

func TestSpannerFilterStatVarsByEntity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "datasource")

	for _, c := range []struct {
		desc       string
		req        *pb.FilterStatVarsByEntityRequest
		mockExist  [][]string
		goldenFile string
	}{
		{
			desc: "Happy path - Filter SVs",
			req: &pb.FilterStatVarsByEntityRequest{
				StatVars: []*pb.EntityInfo{
					{Dcid: "Count_Person"},
					{Dcid: "Median_Income_Person"},
					{Dcid: "NonExistent"},
				},
				Entities: []string{"geoId/06"},
			},
			mockExist: [][]string{
				{"Count_Person", "geoId/06"},
				{"Median_Income_Person", "geoId/06"},
			},
			goldenFile: "filter_stat_vars_happy.json",
		},
		{
			desc: "Empty entities list",
			req: &pb.FilterStatVarsByEntityRequest{
				StatVars: []*pb.EntityInfo{
					{Dcid: "Count_Person"},
				},
				Entities: []string{},
			},
			mockExist:  [][]string{},
			goldenFile: "filter_stat_vars_empty_entities.json",
		},
		{
			desc: "Empty stat vars list",
			req: &pb.FilterStatVarsByEntityRequest{
				StatVars: []*pb.EntityInfo{},
				Entities: []string{"geoId/06"},
			},
			mockExist:  [][]string{},
			goldenFile: "filter_stat_vars_empty_statvars.json",
		},
		{
			desc: "Zero matches from DB",
			req: &pb.FilterStatVarsByEntityRequest{
				StatVars: []*pb.EntityInfo{
					{Dcid: "Count_Person"},
				},
				Entities: []string{"geoId/06"},
			},
			mockExist:  [][]string{},
			goldenFile: "filter_stat_vars_no_matches.json",
		},
		{
			desc: "Multiple entities",
			req: &pb.FilterStatVarsByEntityRequest{
				StatVars: []*pb.EntityInfo{
					{Dcid: "Count_Person"},
					{Dcid: "Median_Income_Person"},
				},
				Entities: []string{"geoId/06", "geoId/08"},
			},
			mockExist: [][]string{
				{"Count_Person", "geoId/06"},
				{"Median_Income_Person", "geoId/08"},
			},
			goldenFile: "filter_stat_vars_multi_entity.json",
		},
	} {
		t.Run(c.desc, func(t *testing.T) {
			client := &mockSpannerClient{
				checkVariableExistenceRes: c.mockExist,
			}
			ds := spanner.NewSpannerDataSource(client, nil, nil, false)

			got, err := ds.FilterStatVarsByEntity(ctx, c.req)
			if err != nil {
				t.Fatalf("FilterStatVarsByEntity error: %v", err)
			}

			if test.GenerateGolden {
				test.UpdateProtoGolden(got, goldenDir, c.goldenFile)
				return
			}

			var want pb.FilterStatVarsByEntityResponse
			if err = test.ReadJSON(goldenDir, c.goldenFile, &want); err != nil {
				t.Fatalf("ReadJSON error (%v): %v", c.goldenFile, err)
			}

			cmpOpts := cmp.Options{
				protocmp.Transform(),
			}
			if diff := cmp.Diff(got, &want, cmpOpts); diff != "" {
				t.Errorf("%v payload mismatch:\n%v", c.goldenFile, diff)
			}
		})
	}
}
