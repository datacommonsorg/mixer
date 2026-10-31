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
	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

type mockV2ObsNode struct {
	name                  string
	url                   string
	types                 []string
	observationProperties []string
}

type mockV2ObservationsServer struct {
	Mixer
	nodes      map[string]mockV2ObsNode
	sdmxResult *sdmxpb.SdmxDataResult
	sdmxErr    error
	errs       map[string]error

	mu             sync.Mutex
	sdmxReqs       []*sdmxpb.SdmxDataQuery
	nodeProperties []string
}

func (m *mockV2ObservationsServer) SdmxData(_ context.Context, in *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error) {
	m.mu.Lock()
	m.sdmxReqs = append(m.sdmxReqs, in)
	m.mu.Unlock()
	if m.sdmxErr != nil {
		return nil, m.sdmxErr
	}
	if m.sdmxResult != nil {
		return m.sdmxResult, nil
	}
	return &sdmxpb.SdmxDataResult{}, nil
}

func (m *mockV2ObservationsServer) V2Node(_ context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	m.mu.Lock()
	m.nodeProperties = append(m.nodeProperties, in.GetProperty())
	m.mu.Unlock()
	if err := m.errs["V2Node"]; err != nil {
		return nil, err
	}
	if err := m.errs["V2Node:"+in.GetProperty()]; err != nil {
		return nil, err
	}

	propSpec := strings.TrimPrefix(in.GetProperty(), "->")
	propSpec = strings.TrimPrefix(propSpec, "[")
	propSpec = strings.TrimSuffix(propSpec, "]")
	var wantedProps []string
	for _, p := range strings.Split(propSpec, ",") {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			wantedProps = append(wantedProps, trimmed)
		}
	}

	resp := &pbv2.NodeResponse{Data: make(map[string]*pbv2.LinkedGraph)}
	for _, dcid := range in.GetNodes() {
		node, ok := m.nodes[dcid]
		if !ok {
			resp.Data[dcid] = &pbv2.LinkedGraph{}
			continue
		}
		arcs := make(map[string]*pbv2.Nodes)
		if slices.Contains(wantedProps, "name") && node.name != "" {
			arcs["name"] = &pbv2.Nodes{Nodes: []*pb.EntityInfo{{Value: node.name}}}
		}
		if slices.Contains(wantedProps, "url") && node.url != "" {
			arcs["url"] = &pbv2.Nodes{Nodes: []*pb.EntityInfo{{Value: node.url}}}
		}
		if slices.Contains(wantedProps, "typeOf") && len(node.types) > 0 {
			typeNodes := make([]*pb.EntityInfo, 0, len(node.types))
			for _, t := range node.types {
				typeNodes = append(typeNodes, &pb.EntityInfo{Dcid: t})
			}
			arcs["typeOf"] = &pbv2.Nodes{Nodes: typeNodes}
		}
		if slices.Contains(wantedProps, "observationProperties") && len(node.observationProperties) > 0 {
			obsNodes := make([]*pb.EntityInfo, 0, len(node.observationProperties))
			for _, p := range node.observationProperties {
				obsNodes = append(obsNodes, &pb.EntityInfo{Value: p})
			}
			arcs["observationProperties"] = &pbv2.Nodes{Nodes: obsNodes}
		}
		resp.Data[dcid] = &pbv2.LinkedGraph{Arcs: arcs}
	}
	return resp, nil
}

func manyDcids(prefix string, n int) []string {
	res := make([]string, 0, n)
	for i := range n {
		res = append(res, fmt.Sprintf("%s_%d", prefix, i))
	}
	return res
}

func int32Ptr(v int32) *int32 {
	return &v
}

func TestV2GetObservationsValidation(t *testing.T) {
	tests := []struct {
		name    string
		req     *V2GetObservationsRequest
		wantErr error
	}{
		{
			name:    "nil request rejected as empty variable_dcids",
			req:     nil,
			wantErr: errV2NoVariableDcids(),
		},
		{
			name: "blank variable_dcids rejected",
			req: &V2GetObservationsRequest{
				VariableDcids: []string{" ", ""},
				EntityDcids:   []string{"geoId/06"},
			},
			wantErr: errV2NoVariableDcids(),
		},
		{
			name: "topic prefix rejected before backend call",
			req: &V2GetObservationsRequest{
				VariableDcids: []string{"Count_Person", "dc/topic/Health"},
				EntityDcids:   []string{"geoId/06"},
			},
			wantErr: errV2TopicAsVariableDcids([]string{"dc/topic/Health"}),
		},
		{
			name: "more than 10 variable_dcids rejected",
			req: &V2GetObservationsRequest{
				VariableDcids: manyDcids("Var", 11),
				EntityDcids:   []string{"geoId/06"},
			},
			wantErr: errV2TooManyVariableDcids(11),
		},
		{
			name: "more than 2 variable_dcids in child mode rejected",
			req: &V2GetObservationsRequest{
				VariableDcids:       manyDcids("Var", 3),
				EntityDcids:         []string{"geoId/06"},
				ChildEntityTypeDcid: "County",
			},
			wantErr: errV2TooManyChildVariableDcids(3),
		},
		{
			name: "source_override with 2 variables rejected",
			req: &V2GetObservationsRequest{
				VariableDcids:  []string{"Count_Person", "Median_Age_Person"},
				EntityDcids:    []string{"geoId/06"},
				SourceOverride: "facet-1",
			},
			wantErr: errV2SourceOverrideNeedsOneVariable(2),
		},
		{
			name: "source_override takes precedence over child mode variable cap",
			req: &V2GetObservationsRequest{
				VariableDcids:       manyDcids("Var", 3),
				EntityDcids:         []string{"geoId/06"},
				ChildEntityTypeDcid: "County",
				SourceOverride:      "facet-1",
			},
			wantErr: errV2SourceOverrideNeedsOneVariable(3),
		},
		{
			name: "empty entity_dcids rejected",
			req: &V2GetObservationsRequest{
				VariableDcids: []string{"Count_Person"},
				EntityDcids:   []string{"  "},
			},
			wantErr: errV2NoEntityDcids(),
		},
		{
			name: "child mode with zero parents rejected",
			req: &V2GetObservationsRequest{
				VariableDcids:       []string{"Count_Person"},
				ChildEntityTypeDcid: "County",
			},
			wantErr: errV2ChildModeNeedsOneEntity(0),
		},
		{
			name: "child mode with two parents rejected",
			req: &V2GetObservationsRequest{
				VariableDcids:       []string{"Count_Person"},
				EntityDcids:         []string{"geoId/06", "geoId/48"},
				ChildEntityTypeDcid: "County",
			},
			wantErr: errV2ChildModeNeedsOneEntity(2),
		},
		{
			name: "more than 20 entity_dcids rejected",
			req: &V2GetObservationsRequest{
				VariableDcids: []string{"Count_Person"},
				EntityDcids:   manyDcids("geoId", 21),
			},
			wantErr: errV2TooManyEntityDcids(21),
		},
		{
			name: "reserved entity_property rejected",
			req: &V2GetObservationsRequest{
				VariableDcids:  []string{"Count_Person"},
				EntityDcids:    []string{"geoId/06"},
				EntityProperty: "TIME_PERIOD",
			},
			wantErr: errV2ReservedEntityProperty("TIME_PERIOD"),
		},
		{
			name: "reserved additional_entities key rejected",
			req: &V2GetObservationsRequest{
				VariableDcids:      []string{"Count_Migration"},
				EntityDcids:        []string{"country/USA"},
				EntityProperty:     "sourceCountry",
				AdditionalEntities: map[string][]string{"facetId": {"f1"}},
			},
			wantErr: errV2ReservedEntityProperty("facetId"),
		},
		{
			name: "duplicate entity_property in additional_entities rejected",
			req: &V2GetObservationsRequest{
				VariableDcids:      []string{"Count_Migration"},
				EntityDcids:        []string{"country/USA"},
				EntityProperty:     "donor",
				AdditionalEntities: map[string][]string{"donor": {"country/KEN"}},
			},
			wantErr: errV2DuplicateEntityProperty("donor"),
		},
		{
			name: "default entity_property colliding with additional_entities observationAbout rejected",
			req: &V2GetObservationsRequest{
				VariableDcids:      []string{"Count_FarmShipment"},
				EntityDcids:        []string{"geoId/06001"},
				AdditionalEntities: map[string][]string{"observationAbout": {"geoId/06085"}},
			},
			wantErr: errV2DuplicateEntityProperty("observationAbout"),
		},
		{
			name: "more than 2 additional_entities keys rejected",
			req: &V2GetObservationsRequest{
				VariableDcids:  []string{"Count_Migration"},
				EntityDcids:    []string{"country/USA"},
				EntityProperty: "p1",
				AdditionalEntities: map[string][]string{
					"p2": {"a"},
					"p3": {"b"},
					"p4": {"c"},
				},
			},
			wantErr: errV2TooManyAdditionalEntityProps(3),
		},
		{
			name: "empty additional_entities slice rejected",
			req: &V2GetObservationsRequest{
				VariableDcids:      []string{"Count_Migration"},
				EntityDcids:        []string{"country/USA"},
				EntityProperty:     "donor",
				AdditionalEntities: map[string][]string{"recipient": {"  "}},
			},
			wantErr: errV2EmptyAdditionalEntityDcids("recipient"),
		},
		{
			name: "more than 20 DCIDs in additional_entities rejected",
			req: &V2GetObservationsRequest{
				VariableDcids:      []string{"Count_Migration"},
				EntityDcids:        []string{"country/USA"},
				EntityProperty:     "donor",
				AdditionalEntities: map[string][]string{"recipient": manyDcids("country", 21)},
			},
			wantErr: errV2TooManyAdditionalEntityDcids("recipient", 21),
		},
		{
			name: "conflicting date=latest with date_range_start rejected",
			req: &V2GetObservationsRequest{
				VariableDcids:  []string{"Count_Person"},
				EntityDcids:    []string{"geoId/06"},
				Date:           "LATEST",
				DateRangeStart: "2020",
			},
			wantErr: errV2ConflictingDateRange("LATEST"),
		},
		{
			name: "conflicting specific date with date_range_end rejected",
			req: &V2GetObservationsRequest{
				VariableDcids: []string{"Count_Person"},
				EntityDcids:   []string{"geoId/06"},
				Date:          "2020",
				DateRangeEnd:  "2023",
			},
			wantErr: errV2ConflictingDateRange("2020"),
		},
		{
			name: "date=range without bounds rejected",
			req: &V2GetObservationsRequest{
				VariableDcids: []string{"Count_Person"},
				EntityDcids:   []string{"geoId/06"},
				Date:          "range",
			},
			wantErr: errV2MissingDateRangeBounds(),
		},
		{
			name: "date=all in child mode rejected",
			req: &V2GetObservationsRequest{
				VariableDcids:       []string{"Count_Person"},
				EntityDcids:         []string{"geoId/06"},
				ChildEntityTypeDcid: "County",
				Date:                "ALL",
			},
			wantErr: errV2UnboundedChildDate(),
		},
		{
			name: "open-start range in child mode rejected",
			req: &V2GetObservationsRequest{
				VariableDcids:       []string{"Count_Person"},
				EntityDcids:         []string{"geoId/06"},
				ChildEntityTypeDcid: "County",
				DateRangeEnd:        "2023",
			},
			wantErr: errV2UnboundedChildDate(),
		},
		{
			name: "invalid date string rejected",
			req: &V2GetObservationsRequest{
				VariableDcids: []string{"Count_Person"},
				EntityDcids:   []string{"geoId/06"},
				Date:          "last-year",
			},
			wantErr: errV2InvalidDate("last-year"),
		},
		{
			name: "invalid date_range_start rejected",
			req: &V2GetObservationsRequest{
				VariableDcids:  []string{"Count_Person"},
				EntityDcids:    []string{"geoId/06"},
				DateRangeStart: "2023-13",
			},
			wantErr: errV2InvalidDateRangeBound("date_range_start", "2023-13"),
		},
		{
			name: "inverted date range rejected",
			req: &V2GetObservationsRequest{
				VariableDcids:  []string{"Count_Person"},
				EntityDcids:    []string{"geoId/06"},
				DateRangeStart: "2024",
				DateRangeEnd:   "2020",
			},
			wantErr: errV2InvertedDateRange("2024", "2020"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockV2ObservationsServer{}
			svc := NewService(mock, NewCache(mock), nil)
			_, err := svc.V2GetObservations(context.Background(), tc.req)
			wantSt := status.Convert(tc.wantErr)
			gotSt := status.Convert(err)
			if gotSt.Code() != wantSt.Code() || gotSt.Message() != wantSt.Message() {
				t.Fatalf("V2GetObservations() error = (%v, %q), want (%v, %q)",
					gotSt.Code(), gotSt.Message(), wantSt.Code(), wantSt.Message())
			}
			if len(mock.sdmxReqs) != 0 {
				t.Fatalf("expected 0 SdmxData calls on validation error, got %d", len(mock.sdmxReqs))
			}
		})
	}
}

func TestBuildV2SdmxDataQuery(t *testing.T) {
	tests := []struct {
		name string
		req  *V2GetObservationsRequest
		want *sdmxpb.SdmxDataQuery
	}{
		{
			name: "explicit entities and default latest pushes TIME_PERIOD=LATEST and deduplicates in order",
			req: &V2GetObservationsRequest{
				VariableDcids: []string{" Count_Person ", "UnemploymentRate_Person", "Count_Person"},
				EntityDcids:   []string{"country/USA", "country/CAN", "country/USA"},
			},
			want: &sdmxpb.SdmxDataQuery{
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
					datacommons.ComponentVariableMeasured: makeSdmxConstraint("Count_Person", "UnemploymentRate_Person"),
					datacommons.ComponentObservationAbout: makeSdmxConstraint("country/USA", "country/CAN"),
					datacommons.ComponentTimePeriod:       makeSdmxConstraint("LATEST"),
				},
			},
		},
		{
			name: "specific date omits TIME_PERIOD from SdmxDataQuery so partial ISO dates filter in Go",
			req: &V2GetObservationsRequest{
				VariableDcids:  []string{"Count_Person"},
				EntityDcids:    []string{"country/USA"},
				Date:           "2020",
				SourceOverride: "facet-9",
			},
			want: &sdmxpb.SdmxDataQuery{
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
					datacommons.ComponentVariableMeasured: makeSdmxConstraint("Count_Person"),
					datacommons.ComponentObservationAbout: makeSdmxConstraint("country/USA"),
					datacommons.ComponentFacetID:          makeSdmxConstraint("facet-9"),
				},
			},
		},
		{
			name: "child mode with custom entity_property and additional_entities",
			req: &V2GetObservationsRequest{
				VariableDcids:       []string{"Count_FarmShipment"},
				EntityDcids:         []string{"geoId/06"},
				ChildEntityTypeDcid: "County",
				EntityProperty:      "sourceCounty",
				AdditionalEntities:  map[string][]string{"observationAbout": {"geoId/06085"}},
				DateRangeStart:      "2019",
			},
			want: &sdmxpb.SdmxDataQuery{
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
					datacommons.ComponentVariableMeasured: makeSdmxConstraint("Count_FarmShipment"),
					"sourceCounty": {
						PropertyConstraints: map[string]*sdmxpb.SdmxPropertyConstraint{
							datacommons.PropertyContainedInPlace: {
								Transitive: true,
								Predicates: makeSdmxPredicates("geoId/06"),
							},
							datacommons.PropertyTypeOf: {
								Predicates: makeSdmxPredicates("County"),
							},
						},
					},
					"observationAbout": makeSdmxConstraint("geoId/06085"),
				},
			},
		},
		{
			name: "accepts up to 10 variable_dcids for explicit entities",
			req: &V2GetObservationsRequest{
				VariableDcids: manyDcids("Var", 10),
				EntityDcids:   []string{"country/BGD"},
			},
			want: &sdmxpb.SdmxDataQuery{
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
					datacommons.ComponentVariableMeasured: makeSdmxConstraint(manyDcids("Var", 10)...),
					datacommons.ComponentObservationAbout: makeSdmxConstraint("country/BGD"),
					datacommons.ComponentTimePeriod:       makeSdmxConstraint("LATEST"),
				},
			},
		},
		{
			name: "colliding untrimmed additional_entities keys merge deterministically in sorted raw key order",
			req: &V2GetObservationsRequest{
				VariableDcids:  []string{"Amount_ODA"},
				EntityDcids:    []string{"country/USA"},
				EntityProperty: "donor",
				AdditionalEntities: map[string][]string{
					"recipient":   {"country/CAN", "country/KEN"},
					" recipient ": {"country/KEN", "country/FRA"},
				},
			},
			want: &sdmxpb.SdmxDataQuery{
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
					datacommons.ComponentVariableMeasured: makeSdmxConstraint("Amount_ODA"),
					"donor":                               makeSdmxConstraint("country/USA"),
					"recipient":                           makeSdmxConstraint("country/KEN", "country/FRA", "country/CAN"),
					datacommons.ComponentTimePeriod:       makeSdmxConstraint("LATEST"),
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			norm := normalizeV2GetObservationsRequest(tc.req)
			filter, err := validateV2GetObservationsRequest(norm)
			if err != nil {
				t.Fatalf("validateV2GetObservationsRequest() unexpected error: %v", err)
			}
			got := buildV2SdmxDataQuery(norm, filter)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("buildV2SdmxDataQuery() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestV2GetObservations(t *testing.T) {
	baseNodes := map[string]mockV2ObsNode{
		"Count_Person":           {name: "Total Population", types: []string{"StatisticalVariable"}},
		"Percent_Person_Obesity": {name: "Obesity Prevalence", types: []string{"StatisticalVariable"}},
		"Area_Farm_Irrigated":    {name: "Area of Irrigated Farms", types: []string{"StatisticalVariable"}},
		"Amount_ODA":             {name: "Gross Bilateral ODA", types: []string{"StatisticalVariable"}, observationProperties: []string{"donor", "recipient"}},
		"Count_FarmShipment":     {name: "Farm Shipments", types: []string{"StatisticalVariable"}, observationProperties: []string{"observationAbout", "sourceCounty"}},
		"country/USA":            {name: "United States", types: []string{"Country"}},
		"country/CAN":            {name: "Canada", types: []string{"Country"}},
		"country/KEN":            {name: "Kenya", types: []string{"Country"}},
		"geoId/06":               {name: "California", types: []string{"State"}},
		"geoId/06085":            {name: "Santa Clara County", types: []string{"County"}},
		"geoId/06001":            {name: "Alameda County", types: []string{"County"}},
		"dc/base/CensusPEP":      {name: "US Census PEP", url: "https://www.census.gov/programs-surveys/popest.html"},
		"dc/base/WorldBank":      {name: "World Bank WDI", url: "https://data.worldbank.org/"},
		"dc/base/CDC_Places":     {name: "", url: "https://www.cdc.gov/places/"}, // exercises provenance DCID fallback when ->name is empty while retaining ->url
		"dc/base/OECD_DAC":       {name: "OECD DAC", url: "https://www.oecd.org/dac/"},
		"dc/base/USDA_Flow":      {name: "USDA Commodity Flow", url: "https://www.usda.gov/flow"},
	}

	tests := []struct {
		name       string
		req        *V2GetObservationsRequest
		sdmxResult *sdmxpb.SdmxDataResult
		errs       map[string]error
		want       *V2GetObservationsResponse
	}{
		{
			name: "multi-variable request preserves order, shares entity_metadata, ranks facets, and populates scaling_factor",
			req: &V2GetObservationsRequest{
				VariableDcids: []string{"Percent_Person_Obesity", "Count_Person"},
				EntityDcids:   []string{"country/USA", "country/CAN"},
			},
			sdmxResult: &sdmxpb.SdmxDataResult{
				Series: []*sdmxpb.SdmxTimeSeries{
					{
						// WorldBank covers both USA and CAN -> beats CensusPEP (only USA) via sortFacetStats.
						Dimensions: map[string]string{
							"variableMeasured":  "Count_Person",
							"observationAbout":  "country/USA",
							"provenance":        "dc/base/WorldBank",
							"unit":              "Person",
							"observationPeriod": "P1Y",
						},
						Attributes: map[string]string{"facetId": "wb-facet"},
						Points:     []*sdmxpb.SdmxDataPoint{{TimePeriod: "2023", ObservationValue: "334914895"}},
					},
					{
						Dimensions: map[string]string{
							"variableMeasured":  "Count_Person",
							"observationAbout":  "country/CAN",
							"provenance":        "dc/base/WorldBank",
							"unit":              "Person",
							"observationPeriod": "P1Y",
						},
						Attributes: map[string]string{"facetId": "wb-facet"},
						Points:     []*sdmxpb.SdmxDataPoint{{TimePeriod: "2023", ObservationValue: "40097761"}},
					},
					{
						Dimensions: map[string]string{
							"variableMeasured":  "Count_Person",
							"observationAbout":  "country/USA",
							"provenance":        "dc/base/CensusPEP",
							"unit":              "Person",
							"measurementMethod": "CensusPEPSurvey",
						},
						Attributes: map[string]string{"facetId": "pep-facet"},
						Points:     []*sdmxpb.SdmxDataPoint{{TimePeriod: "2024", ObservationValue: "340110988"}},
					},
					{
						Dimensions: map[string]string{
							"variableMeasured":  "Percent_Person_Obesity",
							"observationAbout":  "country/USA",
							"provenance":        "dc/base/CDC_Places",
							"unit":              "Percent",
							"measurementMethod": "NotApplicable",
						},
						Attributes: map[string]string{
							"facetId":       "cdc-facet",
							"scalingFactor": "100",
						},
						Points: []*sdmxpb.SdmxDataPoint{{TimePeriod: "2022", ObservationValue: "33.6"}},
					},
				},
			},
			want: &V2GetObservationsResponse{
				EntityMetadata: &Table{
					Columns: []string{"dcid", "name"},
					Rows: [][]any{
						{"country/CAN", "Canada"},
						{"country/USA", "United States"},
					},
				},
				Observations: []*V2VariableObservations{
					{
						Variable: &ObservationNode{Dcid: "Percent_Person_Obesity", Name: "Obesity Prevalence"},
						Data: &Table{
							Columns: []string{"observationAbout", "date", "value"},
							Rows:    [][]any{{"country/USA", "2022", 33.6}},
						},
						SourceMetadata: &V2ObservationSourceMeta{
							SourceID:      "cdc-facet",
							ImportName:    "dc/base/CDC_Places",
							ProvenanceURL: "https://www.cdc.gov/places/",
							Unit:          "Percent",
							ScalingFactor: "100",
						},
					},
					{
						Variable: &ObservationNode{Dcid: "Count_Person", Name: "Total Population"},
						Data: &Table{
							Columns: []string{"observationAbout", "date", "value"},
							Rows: [][]any{
								{"country/CAN", "2023", 40097761.0},
								{"country/USA", "2023", 334914895.0},
							},
						},
						SourceMetadata: &V2ObservationSourceMeta{
							SourceID:          "wb-facet",
							ImportName:        "World Bank WDI",
							ProvenanceURL:     "https://data.worldbank.org/",
							Unit:              "Person",
							ObservationPeriod: "P1Y",
						},
						AlternativeSources: []*V2AlternativeSource{
							{
								SourceMetadata: &V2ObservationSourceMeta{
									SourceID:          "pep-facet",
									ImportName:        "US Census PEP",
									ProvenanceURL:     "https://www.census.gov/programs-surveys/popest.html",
									Unit:              "Person",
									MeasurementMethod: "CensusPEPSurvey",
								},
								EntitiesFoundCount: int32Ptr(1),
							},
						},
					},
				},
			},
		},
		{
			name: "partial availability keeps populated variable and sets factual note on empty variable",
			req: &V2GetObservationsRequest{
				VariableDcids: []string{"Count_Person", "Area_Farm_Irrigated"},
				EntityDcids:   []string{"country/USA"},
			},
			sdmxResult: &sdmxpb.SdmxDataResult{
				Series: []*sdmxpb.SdmxTimeSeries{
					{
						Dimensions: map[string]string{
							"variableMeasured": "Count_Person",
							"observationAbout": "country/USA",
							"provenance":       "dc/base/CensusPEP",
						},
						Attributes: map[string]string{"facetId": "pep-facet"},
						Points:     []*sdmxpb.SdmxDataPoint{{TimePeriod: "2024", ObservationValue: "340110988"}},
					},
				},
			},
			want: &V2GetObservationsResponse{
				EntityMetadata: &Table{
					Columns: []string{"dcid", "name"},
					Rows:    [][]any{{"country/USA", "United States"}},
				},
				Observations: []*V2VariableObservations{
					{
						Variable: &ObservationNode{Dcid: "Count_Person", Name: "Total Population"},
						Data: &Table{
							Columns: []string{"observationAbout", "date", "value"},
							Rows:    [][]any{{"country/USA", "2024", 340110988.0}},
						},
						SourceMetadata: &V2ObservationSourceMeta{
							SourceID:      "pep-facet",
							ImportName:    "US Census PEP",
							ProvenanceURL: "https://www.census.gov/programs-surveys/popest.html",
						},
					},
					{
						Variable: &ObservationNode{Dcid: "Area_Farm_Irrigated", Name: "Area of Irrigated Farms"},
						Data:     &Table{Columns: []string{"observationAbout", "date", "value"}},
						Note:     "no observations found for Area_Farm_Irrigated on the given entity_dcids.",
					},
				},
			},
		},
		{
			name: "multi-entity unconstrained counterpart resolves all counterpart entity names in entity_metadata",
			req: &V2GetObservationsRequest{
				VariableDcids:  []string{"Amount_ODA"},
				EntityDcids:    []string{"country/USA"},
				EntityProperty: "donor",
				DateRangeStart: "2021",
				DateRangeEnd:   "2022",
			},
			sdmxResult: &sdmxpb.SdmxDataResult{
				Series: []*sdmxpb.SdmxTimeSeries{
					{
						Dimensions: map[string]string{
							"variableMeasured": "Amount_ODA",
							"donor":            "country/USA",
							"recipient":        "country/KEN",
							"provenance":       "dc/base/OECD_DAC",
							"unit":             "USDollar",
						},
						Attributes: map[string]string{"facetId": "oecd"},
						Points: []*sdmxpb.SdmxDataPoint{
							{TimePeriod: "2020", ObservationValue: "800"},
							{TimePeriod: "2022", ObservationValue: "950"},
						},
					},
					{
						Dimensions: map[string]string{
							"variableMeasured": "Amount_ODA",
							"donor":            "country/USA",
							"recipient":        "country/CAN",
							"provenance":       "dc/base/OECD_DAC",
							"unit":             "USDollar",
						},
						Attributes: map[string]string{"facetId": "oecd"},
						Points: []*sdmxpb.SdmxDataPoint{
							{TimePeriod: "2021", ObservationValue: "120"},
						},
					},
				},
			},
			want: &V2GetObservationsResponse{
				EntityMetadata: &Table{
					Columns: []string{"dcid", "name"},
					Rows: [][]any{
						{"country/CAN", "Canada"},
						{"country/KEN", "Kenya"},
						{"country/USA", "United States"},
					},
				},
				Observations: []*V2VariableObservations{
					{
						Variable: &ObservationNode{Dcid: "Amount_ODA", Name: "Gross Bilateral ODA"},
						Data: &Table{
							Columns: []string{"donor", "recipient", "date", "value"},
							Rows: [][]any{
								{"country/USA", "country/CAN", "2021", 120.0},
								{"country/USA", "country/KEN", "2022", 950.0},
							},
						},
						SourceMetadata: &V2ObservationSourceMeta{
							SourceID:      "oecd",
							ImportName:    "OECD DAC",
							ProvenanceURL: "https://www.oecd.org/dac/",
							Unit:          "USDollar",
						},
					},
				},
			},
		},
		{
			name: "single bilateral pair with alternative facet omits entities_found_count while resolving both entities in entity_metadata",
			req: &V2GetObservationsRequest{
				VariableDcids:      []string{"Amount_ODA"},
				EntityDcids:        []string{"country/USA"},
				EntityProperty:     "donor",
				AdditionalEntities: map[string][]string{"recipient": {"country/KEN"}},
				DateRangeStart:     "2021",
				DateRangeEnd:       "2022",
			},
			sdmxResult: &sdmxpb.SdmxDataResult{
				Series: []*sdmxpb.SdmxTimeSeries{
					{
						Dimensions: map[string]string{
							"variableMeasured": "Amount_ODA",
							"donor":            "country/USA",
							"recipient":        "country/KEN",
							"provenance":       "dc/base/OECD_DAC",
							"unit":             "USDollar",
						},
						Attributes: map[string]string{"facetId": "oecd"},
						Points: []*sdmxpb.SdmxDataPoint{
							{TimePeriod: "2021", ObservationValue: "900"},
							{TimePeriod: "2022", ObservationValue: "950"},
						},
					},
					{
						Dimensions: map[string]string{
							"variableMeasured": "Amount_ODA",
							"donor":            "country/USA",
							"recipient":        "country/KEN",
							"provenance":       "dc/base/WorldBank",
							"unit":             "USDollar",
						},
						Attributes: map[string]string{"facetId": "wb-oda"},
						Points: []*sdmxpb.SdmxDataPoint{
							{TimePeriod: "2022", ObservationValue: "945"},
						},
					},
				},
			},
			want: &V2GetObservationsResponse{
				EntityMetadata: &Table{
					Columns: []string{"dcid", "name"},
					Rows: [][]any{
						{"country/KEN", "Kenya"},
						{"country/USA", "United States"},
					},
				},
				Observations: []*V2VariableObservations{
					{
						Variable: &ObservationNode{Dcid: "Amount_ODA", Name: "Gross Bilateral ODA"},
						Data: &Table{
							Columns: []string{"donor", "recipient", "date", "value"},
							Rows: [][]any{
								{"country/USA", "country/KEN", "2021", 900.0},
								{"country/USA", "country/KEN", "2022", 950.0},
							},
						},
						SourceMetadata: &V2ObservationSourceMeta{
							SourceID:      "oecd",
							ImportName:    "OECD DAC",
							ProvenanceURL: "https://www.oecd.org/dac/",
							Unit:          "USDollar",
						},
						AlternativeSources: []*V2AlternativeSource{
							{
								SourceMetadata: &V2ObservationSourceMeta{
									SourceID:      "wb-oda",
									ImportName:    "World Bank WDI",
									ProvenanceURL: "https://data.worldbank.org/",
									Unit:          "USDollar",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "county-to-county flow ranks facets by distinct entity tuples rather than flat entity set",
			req: &V2GetObservationsRequest{
				VariableDcids:  []string{"Count_FarmShipment"},
				EntityDcids:    []string{"geoId/06085", "geoId/06001"},
				EntityProperty: "sourceCounty",
				DateRangeStart: "2018",
				DateRangeEnd:   "2022",
			},
			sdmxResult: &sdmxpb.SdmxDataResult{
				Series: []*sdmxpb.SdmxTimeSeries{
					{
						Dimensions: map[string]string{
							"variableMeasured": "Count_FarmShipment",
							"observationAbout": "geoId/06085",
							"sourceCounty":     "geoId/06085",
							"provenance":       "dc/base/USDA_Flow",
							"unit":             "Ton",
						},
						Attributes: map[string]string{"facetId": "facet-a"},
						Points:     []*sdmxpb.SdmxDataPoint{{TimePeriod: "2022", ObservationValue: "10"}},
					},
					{
						Dimensions: map[string]string{
							"variableMeasured": "Count_FarmShipment",
							"observationAbout": "geoId/06085",
							"sourceCounty":     "geoId/06001",
							"provenance":       "dc/base/USDA_Flow",
							"unit":             "Ton",
						},
						Attributes: map[string]string{"facetId": "facet-a"},
						Points:     []*sdmxpb.SdmxDataPoint{{TimePeriod: "2022", ObservationValue: "20"}},
					},
					{
						Dimensions: map[string]string{
							"variableMeasured": "Count_FarmShipment",
							"observationAbout": "geoId/06001",
							"sourceCounty":     "geoId/06085",
							"provenance":       "dc/base/USDA_Flow",
							"unit":             "Ton",
						},
						Attributes: map[string]string{"facetId": "facet-a"},
						Points:     []*sdmxpb.SdmxDataPoint{{TimePeriod: "2022", ObservationValue: "30"}},
					},
					{
						Dimensions: map[string]string{
							"variableMeasured": "Count_FarmShipment",
							"observationAbout": "geoId/06001",
							"sourceCounty":     "geoId/06001",
							"provenance":       "dc/base/USDA_Flow",
							"unit":             "Ton",
						},
						Attributes: map[string]string{"facetId": "facet-a"},
						Points:     []*sdmxpb.SdmxDataPoint{{TimePeriod: "2022", ObservationValue: "40"}},
					},
					{
						// facet-b touches both 06001 and 06085 in a single pair with more dates (5 > 4),
						// but loses primary selection because tuple count is 1 < 4.
						Dimensions: map[string]string{
							"variableMeasured": "Count_FarmShipment",
							"observationAbout": "geoId/06001",
							"sourceCounty":     "geoId/06085",
							"provenance":       "dc/base/CensusPEP",
							"unit":             "Ton",
						},
						Attributes: map[string]string{"facetId": "facet-b"},
						Points: []*sdmxpb.SdmxDataPoint{
							{TimePeriod: "2018", ObservationValue: "25"},
							{TimePeriod: "2019", ObservationValue: "26"},
							{TimePeriod: "2020", ObservationValue: "27"},
							{TimePeriod: "2021", ObservationValue: "28"},
							{TimePeriod: "2022", ObservationValue: "29"},
						},
					},
				},
			},
			want: &V2GetObservationsResponse{
				EntityMetadata: &Table{
					Columns: []string{"dcid", "name"},
					Rows: [][]any{
						{"geoId/06001", "Alameda County"},
						{"geoId/06085", "Santa Clara County"},
					},
				},
				Observations: []*V2VariableObservations{
					{
						Variable: &ObservationNode{Dcid: "Count_FarmShipment", Name: "Farm Shipments"},
						Data: &Table{
							Columns: []string{"observationAbout", "sourceCounty", "date", "value"},
							Rows: [][]any{
								{"geoId/06001", "geoId/06001", "2022", 40.0},
								{"geoId/06001", "geoId/06085", "2022", 30.0},
								{"geoId/06085", "geoId/06001", "2022", 20.0},
								{"geoId/06085", "geoId/06085", "2022", 10.0},
							},
						},
						SourceMetadata: &V2ObservationSourceMeta{
							SourceID:      "facet-a",
							ImportName:    "USDA Commodity Flow",
							ProvenanceURL: "https://www.usda.gov/flow",
							Unit:          "Ton",
						},
						AlternativeSources: []*V2AlternativeSource{
							{
								SourceMetadata: &V2ObservationSourceMeta{
									SourceID:      "facet-b",
									ImportName:    "US Census PEP",
									ProvenanceURL: "https://www.census.gov/programs-surveys/popest.html",
									Unit:          "Ton",
								},
								EntitiesFoundCount: int32Ptr(1),
							},
						},
					},
				},
			},
		},
		{
			name: "open-start inferred range (date_range_end only) keeps observations up to end date",
			req: &V2GetObservationsRequest{
				VariableDcids: []string{"Count_Person"},
				EntityDcids:   []string{"country/USA"},
				DateRangeEnd:  "2020",
			},
			sdmxResult: &sdmxpb.SdmxDataResult{
				Series: []*sdmxpb.SdmxTimeSeries{
					{
						Dimensions: map[string]string{
							"variableMeasured": "Count_Person",
							"observationAbout": "country/USA",
							"provenance":       "dc/base/WorldBank",
						},
						Attributes: map[string]string{"facetId": "wb-facet"},
						Points: []*sdmxpb.SdmxDataPoint{
							{TimePeriod: "2019", ObservationValue: "328000000"},
							{TimePeriod: "2020-06", ObservationValue: "331000000"},
							{TimePeriod: "2021", ObservationValue: "332000000"},
						},
					},
				},
			},
			want: &V2GetObservationsResponse{
				EntityMetadata: &Table{
					Columns: []string{"dcid", "name"},
					Rows:    [][]any{{"country/USA", "United States"}},
				},
				Observations: []*V2VariableObservations{
					{
						Variable: &ObservationNode{Dcid: "Count_Person", Name: "Total Population"},
						Data: &Table{
							Columns: []string{"observationAbout", "date", "value"},
							Rows: [][]any{
								{"country/USA", "2019", 328000000.0},
								{"country/USA", "2020-06", 331000000.0},
							},
						},
						SourceMetadata: &V2ObservationSourceMeta{
							SourceID:      "wb-facet",
							ImportName:    "World Bank WDI",
							ProvenanceURL: "https://data.worldbank.org/",
						},
					},
				},
			},
		},
		{
			name: "date window miss omits source_metadata and reports available date bounds",
			req: &V2GetObservationsRequest{
				VariableDcids:       []string{"Count_Person"},
				EntityDcids:         []string{"geoId/06"},
				ChildEntityTypeDcid: "County",
				Date:                "2025",
			},
			sdmxResult: &sdmxpb.SdmxDataResult{
				Series: []*sdmxpb.SdmxTimeSeries{
					{
						Dimensions: map[string]string{
							"variableMeasured": "Count_Person",
							"observationAbout": "geoId/06085",
							"provenance":       "dc/base/CensusPEP",
						},
						Attributes: map[string]string{"facetId": "pep-facet"},
						Points: []*sdmxpb.SdmxDataPoint{
							{TimePeriod: "2018", ObservationValue: "1900000"},
							{TimePeriod: "2023", ObservationValue: "1880000"},
						},
					},
				},
			},
			want: &V2GetObservationsResponse{
				EntityMetadata: &Table{Columns: []string{"dcid", "name"}},
				Observations: []*V2VariableObservations{
					{
						Variable: &ObservationNode{Dcid: "Count_Person", Name: "Total Population"},
						Data:     &Table{Columns: []string{"observationAbout", "date", "value"}},
						Note:     "no observations found across any source for Count_Person on County entities in geoId/06 in the requested date window (available dates across all sources span 2018 to 2023).",
					},
				},
			},
		},
		{
			name: "date window miss with source_override reports source-specific date bounds",
			req: &V2GetObservationsRequest{
				VariableDcids:  []string{"Count_Person"},
				EntityDcids:    []string{"country/USA"},
				Date:           "2025",
				SourceOverride: "pep-facet",
			},
			sdmxResult: &sdmxpb.SdmxDataResult{
				Series: []*sdmxpb.SdmxTimeSeries{
					{
						Dimensions: map[string]string{
							"variableMeasured": "Count_Person",
							"observationAbout": "country/USA",
							"provenance":       "dc/base/CensusPEP",
						},
						Attributes: map[string]string{"facetId": "pep-facet"},
						Points: []*sdmxpb.SdmxDataPoint{
							{TimePeriod: "2018", ObservationValue: "326000000"},
							{TimePeriod: "2023", ObservationValue: "334000000"},
						},
					},
				},
			},
			want: &V2GetObservationsResponse{
				EntityMetadata: &Table{Columns: []string{"dcid", "name"}},
				Observations: []*V2VariableObservations{
					{
						Variable: &ObservationNode{Dcid: "Count_Person", Name: "Total Population"},
						Data:     &Table{Columns: []string{"observationAbout", "date", "value"}},
						Note:     "no observations found for Count_Person on the given entity_dcids with source_override 'pep-facet' in the requested date window (available dates for this source span 2018 to 2023).",
					},
				},
			},
		},
		{
			name: "zero series with valid inputs and source_override returns source-specific note",
			req: &V2GetObservationsRequest{
				VariableDcids:  []string{"Area_Farm_Irrigated"},
				EntityDcids:    []string{"country/KEN"},
				SourceOverride: "missing-facet",
			},
			sdmxResult: &sdmxpb.SdmxDataResult{},
			want: &V2GetObservationsResponse{
				EntityMetadata: &Table{Columns: []string{"dcid", "name"}},
				Observations: []*V2VariableObservations{
					{
						Variable: &ObservationNode{Dcid: "Area_Farm_Irrigated", Name: "Area of Irrigated Farms"},
						Data:     &Table{Columns: []string{"observationAbout", "date", "value"}},
						Note:     "no observations found for Area_Farm_Irrigated on the given entity_dcids with source_override 'missing-facet'.",
					},
				},
			},
		},
		{
			name: "zero series in explicit mode with additional_entities names both scopes in note",
			req: &V2GetObservationsRequest{
				VariableDcids:      []string{"Amount_ODA"},
				EntityDcids:        []string{"country/USA"},
				EntityProperty:     "donor",
				AdditionalEntities: map[string][]string{"recipient": {"country/KEN"}},
			},
			sdmxResult: &sdmxpb.SdmxDataResult{},
			want: &V2GetObservationsResponse{
				EntityMetadata: &Table{Columns: []string{"dcid", "name"}},
				Observations: []*V2VariableObservations{
					{
						Variable: &ObservationNode{Dcid: "Amount_ODA", Name: "Gross Bilateral ODA"},
						Data:     &Table{Columns: []string{"donor", "recipient", "date", "value"}},
						Note:     "no observations found for Amount_ODA on the given entity_dcids and additional_entities.",
					},
				},
			},
		},
		{
			name: "date window miss in child mode with additional_entities names both scopes in note",
			req: &V2GetObservationsRequest{
				VariableDcids:       []string{"Count_FarmShipment"},
				EntityDcids:         []string{"geoId/06"},
				ChildEntityTypeDcid: "County",
				EntityProperty:      "sourceCounty",
				AdditionalEntities:  map[string][]string{"observationAbout": {"geoId/06085"}},
				Date:                "2025",
			},
			sdmxResult: &sdmxpb.SdmxDataResult{
				Series: []*sdmxpb.SdmxTimeSeries{
					{
						Dimensions: map[string]string{
							"variableMeasured": "Count_FarmShipment",
							"observationAbout": "geoId/06085",
							"sourceCounty":     "geoId/06001",
							"provenance":       "dc/base/USDA_Flow",
						},
						Attributes: map[string]string{"facetId": "facet-a"},
						Points:     []*sdmxpb.SdmxDataPoint{{TimePeriod: "2022", ObservationValue: "20"}},
					},
				},
			},
			want: &V2GetObservationsResponse{
				EntityMetadata: &Table{Columns: []string{"dcid", "name"}},
				Observations: []*V2VariableObservations{
					{
						Variable: &ObservationNode{Dcid: "Count_FarmShipment", Name: "Farm Shipments"},
						Data:     &Table{Columns: []string{"observationAbout", "sourceCounty", "date", "value"}},
						Note:     "no observations found across any source for Count_FarmShipment on County entities in geoId/06 with the given additional_entities in the requested date window (available dates across all sources span 2022 to 2022).",
					},
				},
			},
		},
		{
			name: "zero series in child mode returns child scope note without extra validation RPCs",
			req: &V2GetObservationsRequest{
				VariableDcids:       []string{"Area_Farm_Irrigated"},
				EntityDcids:         []string{"geoId/06"},
				ChildEntityTypeDcid: "County",
			},
			sdmxResult: &sdmxpb.SdmxDataResult{},
			want: &V2GetObservationsResponse{
				EntityMetadata: &Table{Columns: []string{"dcid", "name"}},
				Observations: []*V2VariableObservations{
					{
						Variable: &ObservationNode{Dcid: "Area_Farm_Irrigated", Name: "Area of Irrigated Farms"},
						Data:     &Table{Columns: []string{"observationAbout", "date", "value"}},
						Note:     "no observations found for Area_Farm_Irrigated on County entities in geoId/06.",
					},
				},
			},
		},
		{
			name: "graceful degradation when enrichment V2Node fails still returns observation numbers",
			req: &V2GetObservationsRequest{
				VariableDcids: []string{"Count_Person"},
				EntityDcids:   []string{"country/USA"},
			},
			sdmxResult: &sdmxpb.SdmxDataResult{
				Series: []*sdmxpb.SdmxTimeSeries{
					{
						Dimensions: map[string]string{
							"variableMeasured": "Count_Person",
							"observationAbout": "country/USA",
							"provenance":       "dc/base/CensusPEP",
						},
						Attributes: map[string]string{"facetId": "pep-facet"},
						Points:     []*sdmxpb.SdmxDataPoint{{TimePeriod: "2024", ObservationValue: "340110988"}},
					},
				},
			},
			errs: map[string]error{"V2Node": status.Error(codes.Unavailable, "node down")},
			want: &V2GetObservationsResponse{
				EntityMetadata: &Table{
					Columns: []string{"dcid", "name"},
					Rows:    [][]any{{"country/USA", ""}},
				},
				Observations: []*V2VariableObservations{
					{
						Variable: &ObservationNode{Dcid: "Count_Person"},
						Data: &Table{
							Columns: []string{"observationAbout", "date", "value"},
							Rows:    [][]any{{"country/USA", "2024", 340110988.0}},
						},
						SourceMetadata: &V2ObservationSourceMeta{
							SourceID:   "pep-facet",
							ImportName: "dc/base/CensusPEP",
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockV2ObservationsServer{
				nodes:      baseNodes,
				sdmxResult: tc.sdmxResult,
				errs:       tc.errs,
			}
			svc := NewService(mock, NewCache(mock), nil)
			got, err := svc.V2GetObservations(context.Background(), tc.req)
			if err != nil {
				t.Fatalf("V2GetObservations() unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("V2GetObservations() mismatch (-want +got):\n%s", diff)
			}
			for _, prop := range mock.nodeProperties {
				if strings.Contains(prop, "typeOf") {
					t.Errorf("unexpected typeOf V2Node call %q when SdmxData succeeded", prop)
				}
			}
		})
	}
}

func TestV2GetObservationsDiagnostics(t *testing.T) {
	nodes := map[string]mockV2ObsNode{
		"Count_Person":      {name: "Total Population", types: []string{"StatisticalVariable"}},
		"Amount_ODA":        {name: "Gross Bilateral ODA", types: []string{"StatisticalVariable"}, observationProperties: []string{"donor", "recipient"}},
		"custom/HealthRoot": {name: "Health Topic", types: []string{"Topic"}},
		"dc/g/Farm":         {name: "Farm Group", types: []string{"StatVarGroup"}},
		"country/USA":       {name: "United States", types: []string{"Country"}},
		"geoId/06":          {name: "California", types: []string{"State"}},
	}

	tests := []struct {
		name    string
		req     *V2GetObservationsRequest
		sdmxErr error
		wantErr error
	}{
		{
			name: "mixed observation_properties across variable_dcids",
			req: &V2GetObservationsRequest{
				VariableDcids: []string{"Count_Person", "Amount_ODA"},
				EntityDcids:   []string{"country/USA"},
			},
			sdmxErr: status.Error(codes.InvalidArgument, `resolveSdmxEntityShape: incompatible observationProperties`),
			wantErr: errV2MixedObservationProperties(`Count_Person: ["observationAbout"]; Amount_ODA: ["donor" "recipient"]`),
		},
		{
			name: "wrong default entity_property on multi-entity variable",
			req: &V2GetObservationsRequest{
				VariableDcids: []string{"Amount_ODA"},
				EntityDcids:   []string{"country/USA"},
			},
			sdmxErr: status.Error(codes.InvalidArgument, `unsupported SDMX component filter "observationAbout"`),
			wantErr: errV2WrongObservationProperty("Amount_ODA", []string{"observationAbout"}, []string{"donor", "recipient"}),
		},
		{
			name: "wrong entity_property in child mode (SdmxData returns Unimplemented)",
			req: &V2GetObservationsRequest{
				VariableDcids:       []string{"Amount_ODA"},
				EntityDcids:         []string{"country/USA"},
				ChildEntityTypeDcid: "State",
			},
			sdmxErr: status.Error(codes.Unimplemented, `SDMX property constraints on component "observationAbout" are not implemented yet`),
			wantErr: errV2WrongObservationProperty("Amount_ODA", []string{"observationAbout"}, []string{"donor", "recipient"}),
		},
		{
			name: "Spanner LinkedEdge InvalidArgument never leaks raw text",
			req: &V2GetObservationsRequest{
				VariableDcids:       []string{"Count_Person"},
				EntityDcids:         []string{"geoId/06"},
				ChildEntityTypeDcid: "County",
			},
			sdmxErr: status.Error(codes.InvalidArgument, `spanner: code = "InvalidArgument", desc = "Table not found: LinkedEdge"`),
			wantErr: errV2ObservationsUnavailable(),
		},
		{
			name: "custom topic without dc/topic/ prefix in rejected batch classified as topic",
			req: &V2GetObservationsRequest{
				VariableDcids: []string{"custom/HealthRoot", "Amount_ODA"},
				EntityDcids:   []string{"country/USA"},
			},
			sdmxErr: status.Error(codes.InvalidArgument, `resolveSdmxEntityShape: incompatible observationProperties`),
			wantErr: errV2TopicAsVariableDcids([]string{"custom/HealthRoot"}),
		},
		{
			name: "StatVarGroup dc/g/ prefix in rejected batch classified as unknown variable",
			req: &V2GetObservationsRequest{
				VariableDcids: []string{"dc/g/Farm", "Amount_ODA"},
				EntityDcids:   []string{"country/USA"},
			},
			sdmxErr: status.Error(codes.InvalidArgument, `resolveSdmxEntityShape: incompatible observationProperties`),
			wantErr: errV2UnknownVariableDcids([]string{"dc/g/Farm"}),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockV2ObservationsServer{
				nodes:   nodes,
				sdmxErr: tc.sdmxErr,
			}
			svc := NewService(mock, NewCache(mock), nil)
			_, err := svc.V2GetObservations(context.Background(), tc.req)
			wantSt := status.Convert(tc.wantErr)
			gotSt := status.Convert(err)
			if gotSt.Code() != wantSt.Code() || gotSt.Message() != wantSt.Message() {
				t.Fatalf("V2GetObservations() error = (%v, %q), want (%v, %q)",
					gotSt.Code(), gotSt.Message(), wantSt.Code(), wantSt.Message())
			}
		})
	}
}
