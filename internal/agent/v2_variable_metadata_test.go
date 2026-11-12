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
	"maps"
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

// mockV2MetadataServer extends mockV2ObservationsServer with "->*" node graphs and child entity samples for
// V2GetVariableMetadata tests. Other V2Node queries (SdmxData rejection diagnosis) go to the embedded mock.
type mockV2MetadataServer struct {
	*mockV2ObservationsServer
	graphs    map[string]*pbv2.LinkedGraph
	graphErrs map[string]error    // fails "->*" lookups that include the DCID
	samples   map[string][]string // "parent|childType" -> sampled child DCIDs
	sampleErr error
	afterSdmx func() // runs after each SdmxData call, e.g. to cancel the request

	sampleMu   sync.Mutex
	sampleReqs []*pbv2.SampleChildEntitiesRequest
}

func (m *mockV2MetadataServer) SdmxData(ctx context.Context, in *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error) {
	res, err := m.mockV2ObservationsServer.SdmxData(ctx, in)
	if m.afterSdmx != nil {
		m.afterSdmx()
	}
	return res, err
}

func (m *mockV2MetadataServer) V2Node(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	if in.GetProperty() != wildcardPropertyQuery {
		return m.mockV2ObservationsServer.V2Node(ctx, in)
	}
	for _, node := range in.GetNodes() {
		if err := m.graphErrs[node]; err != nil {
			return nil, err
		}
	}
	resp := &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{}}
	for _, node := range in.GetNodes() {
		if graph, ok := m.graphs[node]; ok {
			resp.Data[node] = graph
		}
	}
	return resp, nil
}

func (m *mockV2MetadataServer) SampleChildEntities(_ context.Context, in *pbv2.SampleChildEntitiesRequest) (*pbv2.SampleChildEntitiesResponse, error) {
	m.sampleMu.Lock()
	m.sampleReqs = append(m.sampleReqs, in)
	m.sampleMu.Unlock()
	if m.sampleErr != nil {
		return nil, m.sampleErr
	}
	resp := &pbv2.SampleChildEntitiesResponse{ChildEntities: map[string]*pbv2.SampleChildEntitiesResponse_ChildEntities{}}
	for _, ancestor := range in.GetAncestors() {
		if children := m.samples[ancestor+"|"+in.GetChildType()]; len(children) > 0 {
			resp.ChildEntities[ancestor] = &pbv2.SampleChildEntitiesResponse_ChildEntities{Dcids: children}
		}
	}
	return resp, nil
}

// metaValues returns arc nodes holding literal values.
func metaValues(values ...string) *pbv2.Nodes {
	nodes := &pbv2.Nodes{}
	for _, v := range values {
		nodes.Nodes = append(nodes.Nodes, &pb.EntityInfo{Value: v})
	}
	return nodes
}

// metaRef returns an arc node referencing another node, with an optional display name.
func metaRef(dcid, name string) *pbv2.Nodes {
	return &pbv2.Nodes{Nodes: []*pb.EntityInfo{{Dcid: dcid, Name: name}}}
}

// metaSeries builds an SDMX series of a variable from one facet, with the given dimensions and one point per
// date.
func metaSeries(variable, facetID string, dims map[string]string, dates ...string) *sdmxpb.SdmxTimeSeries {
	dimensions := map[string]string{"variableMeasured": variable}
	maps.Copy(dimensions, dims)
	series := &sdmxpb.SdmxTimeSeries{Dimensions: dimensions, Attributes: map[string]string{"facetId": facetID}}
	for _, date := range dates {
		series.Points = append(series.Points, &sdmxpb.SdmxDataPoint{TimePeriod: date, ObservationValue: "1"})
	}
	return series
}

// metadataGraphs are the "->*" graphs of the variables, provenances and sources used across tests.
func metadataGraphs() map[string]*pbv2.LinkedGraph {
	return map[string]*pbv2.LinkedGraph{
		"Count_Person": {Arcs: map[string]*pbv2.Nodes{
			"name":             metaValues("Total Population"),
			"description":      metaValues("The number of people."),
			"populationType":   metaRef("Person", ""),
			"measuredProperty": metaRef("count", ""),
			"statType":         metaRef("measuredValue", ""),
			"typeOf":           metaRef("StatisticalVariable", ""),
			"memberOf":         metaRef("dc/g/Demographics", ""),
		}},
		"Median_Age_Person": {Arcs: map[string]*pbv2.Nodes{
			"name":             metaValues("Median Age"),
			"measuredProperty": metaRef("age", ""),
			"statType":         metaRef("medianValue", ""),
		}},
		"Amount_ODA": {Arcs: map[string]*pbv2.Nodes{
			"name":                  metaValues("Gross ODA"),
			"observationProperties": metaValues("donor", "recipient"),
		}},
		"dc/base/WorldBank": {Arcs: map[string]*pbv2.Nodes{
			"name":                   metaValues("World Bank WDI"),
			"url":                    metaValues("https://data.worldbank.org/"),
			"licenseType":            metaRef("CreativeCommonsAttributionLicense", "Creative Commons Attribution License"),
			"source":                 metaRef("dc/s/WorldBank", "World Bank"),
			"sourceReleaseFrequency": metaValues("P1Y"),
			"typeOf":                 metaRef("Provenance", ""),
		}},
		"dc/s/WorldBank": {Arcs: map[string]*pbv2.Nodes{
			"name":   metaValues("World Bank"),
			"url":    metaValues("https://www.worldbank.org/"),
			"domain": metaValues("worldbank.org"),
		}},
		"dc/base/CensusPEP": {Arcs: map[string]*pbv2.Nodes{
			"name":        metaValues("US Census PEP"),
			"url":         metaValues("https://www.census.gov/popest"),
			"description": metaValues("Population estimates"),
		}},
	}
}

var (
	wbDims = func(entity string) map[string]string {
		return map[string]string{"observationAbout": entity, "provenance": "dc/base/WorldBank", "unit": "Person", "observationPeriod": "P1Y"}
	}
	pepDims = func(entity string) map[string]string {
		return map[string]string{"observationAbout": entity, "provenance": "dc/base/CensusPEP", "measurementMethod": "CensusPEPSurvey"}
	}
	countPersonEntry = func(sources *Table, note string) *V2VariableMetadata {
		return &V2VariableMetadata{
			Dcid:        "Count_Person",
			Name:        "Total Population",
			Description: "The number of people.",
			Properties:  map[string]any{"populationType": "Person", "measuredProperty": "count", "statType": "measuredValue"},
			Sources:     sources,
			Note:        note,
		}
	}
	censusPEPDataset = &V2DatasetMetadata{
		ImportName: "US Census PEP",
		Properties: map[string]any{"url": "https://www.census.gov/popest", "description": "Population estimates"},
	}
)

func TestV2GetVariableMetadata(t *testing.T) {
	tests := []struct {
		name           string
		req            *V2GetVariableMetadataRequest
		series         []*sdmxpb.SdmxTimeSeries
		samples        map[string][]string
		graphErrs      map[string]error
		want           *V2GetVariableMetadataResponse
		wantQuery      *sdmxpb.SdmxDataQuery
		wantSampleReqs []*pbv2.SampleChildEntitiesRequest
	}{
		{
			name: "explicit entities: ranked sources with dates, counts and entities, definitions, and datasets listed once",
			req: &V2GetVariableMetadataRequest{
				VariableDcids: []string{"Count_Person", "Median_Age_Person"},
				EntityDcids:   []string{"country/USA", "country/CAN"},
			},
			series: []*sdmxpb.SdmxTimeSeries{
				metaSeries("Count_Person", "pep-facet", pepDims("country/USA"), "2024"),
				metaSeries("Count_Person", "wb-facet", wbDims("country/USA"), "2020", "2023"),
				metaSeries("Count_Person", "wb-facet", wbDims("country/CAN"), "2021", "2023"),
				metaSeries("Median_Age_Person", "wb-age-facet", map[string]string{"observationAbout": "country/CAN", "provenance": "dc/base/WorldBank", "unit": "Year"}, "2019", "2022"),
			},
			want: &V2GetVariableMetadataResponse{
				Variables: []*V2VariableMetadata{
					// wb-facet covers both entities, so it ranks above pep-facet despite older data.
					countPersonEntry(&Table{Columns: v2MetadataSourceColumns, Rows: [][]any{
						{"wb-facet", "World Bank WDI", "https://data.worldbank.org/", "Person", "", "", "P1Y", "2020", "2023", 4, []string{"country/CAN", "country/USA"}},
						{"pep-facet", "US Census PEP", "https://www.census.gov/popest", "", "", "CensusPEPSurvey", "", "2024", "2024", 1, []string{"country/USA"}},
					}}, ""),
					{
						Dcid:       "Median_Age_Person",
						Name:       "Median Age",
						Properties: map[string]any{"measuredProperty": "age", "statType": "medianValue"},
						Sources: &Table{Columns: v2MetadataSourceColumns, Rows: [][]any{
							{"wb-age-facet", "World Bank WDI", "https://data.worldbank.org/", "Year", "", "", "", "2019", "2022", 2, []string{"country/CAN"}},
						}},
					},
				},
				Datasets: []*V2DatasetMetadata{
					{
						// Provenance properties override the parent source's url; the source adds domain.
						ImportName: "World Bank WDI",
						Properties: map[string]any{
							"url":                    "https://data.worldbank.org/",
							"domain":                 "worldbank.org",
							"licenseType":            "Creative Commons Attribution License",
							"source":                 "World Bank",
							"sourceReleaseFrequency": "P1Y",
						},
					},
					censusPEPDataset,
				},
			},
			wantQuery: &sdmxpb.SdmxDataQuery{Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: makeSdmxConstraint("Count_Person", "Median_Age_Person"),
				datacommons.ComponentObservationAbout: makeSdmxConstraint("country/USA", "country/CAN"),
			}},
		},
		{
			name: "child mode queries the sampled children over all dates and reports the parent without counts",
			req: &V2GetVariableMetadataRequest{
				VariableDcids:       []string{"Count_Person", "Count_Farm"},
				EntityDcids:         []string{"geoId/06"},
				ChildEntityTypeDcid: "County",
			},
			samples: map[string][]string{"geoId/06|County": {"geoId/06001", "geoId/06085"}},
			series: []*sdmxpb.SdmxTimeSeries{
				metaSeries("Count_Person", "pep-facet", pepDims("geoId/06001"), "2020", "2022"),
				metaSeries("Count_Person", "pep-facet", pepDims("geoId/06085"), "2021"),
			},
			want: &V2GetVariableMetadataResponse{
				Variables: []*V2VariableMetadata{
					countPersonEntry(&Table{Columns: v2MetadataSourceColumns, Rows: [][]any{
						{"pep-facet", "US Census PEP", "https://www.census.gov/popest", "", "", "CensusPEPSurvey", "", "2020", "2022", nil, []string{"geoId/06"}},
					}}, ""),
					{
						Dcid:       "Count_Farm",
						Properties: map[string]any{},
						Sources:    &Table{Columns: v2MetadataSourceColumns},
						Note:       "no observations found for Count_Farm on County entities in geoId/06.",
					},
				},
				Datasets: []*V2DatasetMetadata{censusPEPDataset},
			},
			wantQuery: &sdmxpb.SdmxDataQuery{Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: makeSdmxConstraint("Count_Person", "Count_Farm"),
				datacommons.ComponentObservationAbout: makeSdmxConstraint("geoId/06001", "geoId/06085"),
			}},
			wantSampleReqs: []*pbv2.SampleChildEntitiesRequest{{Ancestors: []string{"geoId/06"}, ChildType: "County", LimitPerAncestor: v2ChildEntitySampleSize}},
		},
		{
			// get_observations allows 2 variables in child mode; metadata reads only the sampled children, so it
			// allows as many variables as with explicit entities.
			name: "child mode accepts more than 2 variables",
			req: &V2GetVariableMetadataRequest{
				VariableDcids:       []string{"Count_Person", "Median_Age_Person", "Count_Farm"},
				EntityDcids:         []string{"geoId/06"},
				ChildEntityTypeDcid: "County",
			},
			samples: map[string][]string{"geoId/06|County": {"geoId/06001"}},
			series: []*sdmxpb.SdmxTimeSeries{
				metaSeries("Count_Person", "pep-facet", pepDims("geoId/06001"), "2021"),
			},
			want: &V2GetVariableMetadataResponse{
				Variables: []*V2VariableMetadata{
					countPersonEntry(&Table{Columns: v2MetadataSourceColumns, Rows: [][]any{
						{"pep-facet", "US Census PEP", "https://www.census.gov/popest", "", "", "CensusPEPSurvey", "", "2021", "2021", nil, []string{"geoId/06"}},
					}}, ""),
					{
						Dcid:       "Median_Age_Person",
						Name:       "Median Age",
						Properties: map[string]any{"measuredProperty": "age", "statType": "medianValue"},
						Sources:    &Table{Columns: v2MetadataSourceColumns},
						Note:       "no observations found for Median_Age_Person on County entities in geoId/06.",
					},
					{
						Dcid:       "Count_Farm",
						Properties: map[string]any{},
						Sources:    &Table{Columns: v2MetadataSourceColumns},
						Note:       "no observations found for Count_Farm on County entities in geoId/06.",
					},
				},
				Datasets: []*V2DatasetMetadata{censusPEPDataset},
			},
			wantQuery: &sdmxpb.SdmxDataQuery{Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: makeSdmxConstraint("Count_Person", "Median_Age_Person", "Count_Farm"),
				datacommons.ComponentObservationAbout: makeSdmxConstraint("geoId/06001"),
			}},
			wantSampleReqs: []*pbv2.SampleChildEntitiesRequest{{Ancestors: []string{"geoId/06"}, ChildType: "County", LimitPerAncestor: v2ChildEntitySampleSize}},
		},
		{
			name: "multi-entity variable lists entity_property values and counts observations across counterparts",
			req: &V2GetVariableMetadataRequest{
				VariableDcids:  []string{"Amount_ODA"},
				EntityDcids:    []string{"country/USA"},
				EntityProperty: "donor",
			},
			series: []*sdmxpb.SdmxTimeSeries{
				metaSeries("Amount_ODA", "oecd", map[string]string{"donor": "country/USA", "recipient": "country/KEN", "provenance": "dc/base/OECD"}, "2020", "2021"),
				metaSeries("Amount_ODA", "oecd", map[string]string{"donor": "country/USA", "recipient": "country/IND", "provenance": "dc/base/OECD"}, "2021"),
			},
			want: &V2GetVariableMetadataResponse{
				Variables: []*V2VariableMetadata{{
					Dcid:       "Amount_ODA",
					Name:       "Gross ODA",
					Properties: map[string]any{"observationProperties": []string{"donor", "recipient"}},
					// The provenance has no graph, so its DCID is the import name and it has no dataset entry.
					Sources: &Table{Columns: v2MetadataSourceColumns, Rows: [][]any{
						{"oecd", "dc/base/OECD", "", "", "", "", "", "2020", "2021", 3, []string{"country/USA"}},
					}},
				}},
				Datasets: []*V2DatasetMetadata{},
			},
			wantQuery: &sdmxpb.SdmxDataQuery{Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: makeSdmxConstraint("Amount_ODA"),
				"donor":                               makeSdmxConstraint("country/USA"),
			}},
		},
		{
			name: "series without observations add no dates, counts, entities or sources",
			req: &V2GetVariableMetadataRequest{
				VariableDcids: []string{"Count_Person"},
				EntityDcids:   []string{"country/USA", "country/CAN"},
			},
			series: []*sdmxpb.SdmxTimeSeries{
				metaSeries("Count_Person", "pep-facet", pepDims("country/USA"), "2024"),
				metaSeries("Count_Person", "pep-facet", pepDims("country/CAN")),
				metaSeries("Count_Person", "wb-facet", wbDims("country/CAN")),
			},
			want: &V2GetVariableMetadataResponse{
				Variables: []*V2VariableMetadata{countPersonEntry(&Table{Columns: v2MetadataSourceColumns, Rows: [][]any{
					{"pep-facet", "US Census PEP", "https://www.census.gov/popest", "", "", "CensusPEPSurvey", "", "2024", "2024", 1, []string{"country/USA"}},
				}}, "")},
				Datasets: []*V2DatasetMetadata{censusPEPDataset},
			},
			wantQuery: &sdmxpb.SdmxDataQuery{Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: makeSdmxConstraint("Count_Person"),
				datacommons.ComponentObservationAbout: makeSdmxConstraint("country/USA", "country/CAN"),
			}},
		},
		{
			name: "variable property lookup failure keeps sources without definitions",
			req: &V2GetVariableMetadataRequest{
				VariableDcids: []string{"Count_Person"},
				EntityDcids:   []string{"country/USA"},
			},
			series:    []*sdmxpb.SdmxTimeSeries{metaSeries("Count_Person", "pep-facet", pepDims("country/USA"), "2024")},
			graphErrs: map[string]error{"Count_Person": errors.New("node lookup failed")},
			want: &V2GetVariableMetadataResponse{
				Variables: []*V2VariableMetadata{{
					Dcid:       "Count_Person",
					Properties: map[string]any{},
					Sources: &Table{Columns: v2MetadataSourceColumns, Rows: [][]any{
						{"pep-facet", "US Census PEP", "https://www.census.gov/popest", "", "", "CensusPEPSurvey", "", "2024", "2024", 1, []string{"country/USA"}},
					}},
				}},
				Datasets: []*V2DatasetMetadata{censusPEPDataset},
			},
			wantQuery: &sdmxpb.SdmxDataQuery{Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: makeSdmxConstraint("Count_Person"),
				datacommons.ComponentObservationAbout: makeSdmxConstraint("country/USA"),
			}},
		},
		{
			name: "provenance lookup failure falls back to provenance DCIDs and returns no datasets",
			req: &V2GetVariableMetadataRequest{
				VariableDcids: []string{"Count_Person"},
				EntityDcids:   []string{"country/USA"},
			},
			series:    []*sdmxpb.SdmxTimeSeries{metaSeries("Count_Person", "pep-facet", pepDims("country/USA"), "2024")},
			graphErrs: map[string]error{"dc/base/CensusPEP": errors.New("node lookup failed")},
			want: &V2GetVariableMetadataResponse{
				Variables: []*V2VariableMetadata{countPersonEntry(&Table{Columns: v2MetadataSourceColumns, Rows: [][]any{
					{"pep-facet", "dc/base/CensusPEP", "", "", "", "CensusPEPSurvey", "", "2024", "2024", 1, []string{"country/USA"}},
				}}, "")},
				Datasets: []*V2DatasetMetadata{},
			},
			wantQuery: &sdmxpb.SdmxDataQuery{Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: makeSdmxConstraint("Count_Person"),
				datacommons.ComponentObservationAbout: makeSdmxConstraint("country/USA"),
			}},
		},
		{
			name: "source lookup failure keeps the provenance's dataset properties",
			req: &V2GetVariableMetadataRequest{
				VariableDcids: []string{"Median_Age_Person"},
				EntityDcids:   []string{"country/CAN"},
			},
			series: []*sdmxpb.SdmxTimeSeries{
				metaSeries("Median_Age_Person", "wb-age-facet", map[string]string{"observationAbout": "country/CAN", "provenance": "dc/base/WorldBank", "unit": "Year"}, "2022"),
			},
			graphErrs: map[string]error{"dc/s/WorldBank": errors.New("node lookup failed")},
			want: &V2GetVariableMetadataResponse{
				Variables: []*V2VariableMetadata{{
					Dcid:       "Median_Age_Person",
					Name:       "Median Age",
					Properties: map[string]any{"measuredProperty": "age", "statType": "medianValue"},
					Sources: &Table{Columns: v2MetadataSourceColumns, Rows: [][]any{
						{"wb-age-facet", "World Bank WDI", "https://data.worldbank.org/", "Year", "", "", "", "2022", "2022", 1, []string{"country/CAN"}},
					}},
				}},
				// Without the source's properties, domain is missing.
				Datasets: []*V2DatasetMetadata{{
					ImportName: "World Bank WDI",
					Properties: map[string]any{
						"url":                    "https://data.worldbank.org/",
						"licenseType":            "Creative Commons Attribution License",
						"source":                 "World Bank",
						"sourceReleaseFrequency": "P1Y",
					},
				}},
			},
			wantQuery: &sdmxpb.SdmxDataQuery{Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: makeSdmxConstraint("Median_Age_Person"),
				datacommons.ComponentObservationAbout: makeSdmxConstraint("country/CAN"),
			}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockV2MetadataServer{
				mockV2ObservationsServer: &mockV2ObservationsServer{sdmxResult: &sdmxpb.SdmxDataResult{Series: tc.series}},
				graphs:                   metadataGraphs(),
				graphErrs:                tc.graphErrs,
				samples:                  tc.samples,
			}
			got, err := NewService(mock, NewCache(mock), nil).V2GetVariableMetadata(context.Background(), tc.req)
			if err != nil {
				t.Fatalf("V2GetVariableMetadata() unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("V2GetVariableMetadata() mismatch (-want +got):\n%s", diff)
			}
			if len(mock.sdmxReqs) != 1 {
				t.Fatalf("SdmxData calls = %d, want 1", len(mock.sdmxReqs))
			}
			if diff := cmp.Diff(tc.wantQuery, mock.sdmxReqs[0], protocmp.Transform()); diff != "" {
				t.Errorf("SdmxData query mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantSampleReqs, mock.sampleReqs, protocmp.Transform()); diff != "" {
				t.Errorf("SampleChildEntities requests mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestV2GetVariableMetadataRanking checks that sources rows follow V2GetObservations' ranking for the default
// date: its source_metadata first, then its alternative_sources.
func TestV2GetVariableMetadataRanking(t *testing.T) {
	tests := []struct {
		name        string
		entityDcids []string
		series      []*sdmxpb.SdmxTimeSeries
		want        []string
	}{
		{
			// Ranking over all dates (dense-old has more observations) or by facet ID would put dense-old first.
			name:        "the most recent source ranks first among sources covering the same entities",
			entityDcids: []string{"country/USA"},
			series: []*sdmxpb.SdmxTimeSeries{
				metaSeries("Count_Person", "dense-old", wbDims("country/USA"), "2010", "2011", "2012", "2013", "2019"),
				metaSeries("Count_Person", "sparse-new", pepDims("country/USA"), "2023"),
			},
			want: []string{"sparse-new", "dense-old"},
		},
		{
			name:        "entity coverage ranks above recency",
			entityDcids: []string{"country/USA", "country/CAN"},
			series: []*sdmxpb.SdmxTimeSeries{
				metaSeries("Count_Person", "dense-old", wbDims("country/USA"), "2010", "2011", "2012", "2013", "2019"),
				metaSeries("Count_Person", "sparse-new", pepDims("country/USA"), "2023"),
				metaSeries("Count_Person", "wide", wbDims("country/USA"), "2015"),
				metaSeries("Count_Person", "wide", wbDims("country/CAN"), "2016"),
			},
			want: []string{"wide", "sparse-new", "dense-old"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockV2MetadataServer{
				mockV2ObservationsServer: &mockV2ObservationsServer{sdmxResult: &sdmxpb.SdmxDataResult{Series: tc.series}},
				graphs:                   metadataGraphs(),
			}
			svc := NewService(mock, NewCache(mock), nil)

			meta, err := svc.V2GetVariableMetadata(context.Background(),
				&V2GetVariableMetadataRequest{VariableDcids: []string{"Count_Person"}, EntityDcids: tc.entityDcids})
			if err != nil {
				t.Fatalf("V2GetVariableMetadata() unexpected error: %v", err)
			}
			var gotMeta []string
			for _, row := range meta.Variables[0].Sources.Rows {
				gotMeta = append(gotMeta, row[0].(string))
			}
			if diff := cmp.Diff(tc.want, gotMeta); diff != "" {
				t.Errorf("V2GetVariableMetadata() source order mismatch (-want +got):\n%s", diff)
			}

			obs, err := svc.V2GetObservations(context.Background(),
				&V2GetObservationsRequest{VariableDcids: []string{"Count_Person"}, EntityDcids: tc.entityDcids})
			if err != nil {
				t.Fatalf("V2GetObservations() unexpected error: %v", err)
			}
			gotObs := []string{obs.Observations[0].SourceMetadata.SourceID}
			for _, alt := range obs.Observations[0].AlternativeSources {
				gotObs = append(gotObs, alt.SourceMetadata.SourceID)
			}
			if diff := cmp.Diff(tc.want, gotObs); diff != "" {
				t.Errorf("V2GetObservations() source order mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestV2GetVariableMetadataErrors(t *testing.T) {
	sampleTimeout := status.Error(codes.DeadlineExceeded, "spanner: deadline exceeded on session projects/p/sessions/s")
	sdmxUnavailable := status.Error(codes.Unavailable, "spanner: session pool exhausted for projects/p")
	tests := []struct {
		name            string
		req             *V2GetVariableMetadataRequest
		samples         map[string][]string
		sampleErr       error
		sdmxErr         error
		cancelAfterSdmx bool
		wantErr         error
		wantSampleCalls int
		wantSdmxCalls   int
	}{
		{
			name:    "nil request is rejected as empty variable_dcids",
			wantErr: errV2NoVariableDcids(),
		},
		{
			name:    "more than 10 variables in child mode",
			req:     &V2GetVariableMetadataRequest{VariableDcids: manyDcids("Var", 11), EntityDcids: []string{"geoId/06"}, ChildEntityTypeDcid: "County"},
			wantErr: errV2TooManyVariableDcids(11),
		},
		{
			name:    "topic DCID passed as a variable",
			req:     &V2GetVariableMetadataRequest{VariableDcids: []string{"dc/topic/Health"}, EntityDcids: []string{"geoId/06"}},
			wantErr: errV2TopicAsVariableDcids([]string{"dc/topic/Health"}),
		},
		{
			name:    "two parents in child mode",
			req:     &V2GetVariableMetadataRequest{VariableDcids: []string{"Count_Person"}, EntityDcids: []string{"geoId/06", "geoId/48"}, ChildEntityTypeDcid: "County"},
			wantErr: errV2ChildModeNeedsOneEntity(2),
		},
		{
			name:            "parent without child entities of the type",
			req:             &V2GetVariableMetadataRequest{VariableDcids: []string{"Count_Person"}, EntityDcids: []string{"geoId/06"}, ChildEntityTypeDcid: "State"},
			wantErr:         errV2NoChildEntitiesOfType("geoId/06", "State"),
			wantSampleCalls: 1,
		},
		{
			name:            "unknown parent in child mode",
			req:             &V2GetVariableMetadataRequest{VariableDcids: []string{"Count_Person"}, EntityDcids: []string{"geoId/99"}, ChildEntityTypeDcid: "County"},
			wantErr:         errV2UnknownEntityDcids([]string{"geoId/99"}),
			wantSampleCalls: 1,
		},
		{
			// Deployments configured for LinkedEdge without the table fail with InvalidArgument, whose text must not reach the caller.
			name:            "child sampling unavailable",
			req:             &V2GetVariableMetadataRequest{VariableDcids: []string{"Count_Person"}, EntityDcids: []string{"geoId/06"}, ChildEntityTypeDcid: "County"},
			sampleErr:       status.Error(codes.InvalidArgument, "Table not found: LinkedEdge"),
			wantErr:         errV2ChildEntitiesUnavailable(),
			wantSampleCalls: 1,
		},
		{
			name:            "child sampling timeout",
			req:             &V2GetVariableMetadataRequest{VariableDcids: []string{"Count_Person"}, EntityDcids: []string{"geoId/06"}, ChildEntityTypeDcid: "County"},
			sampleErr:       sampleTimeout,
			wantErr:         errV2ChildEntitySamplingFailed(sampleTimeout),
			wantSampleCalls: 1,
		},
		{
			name:          "SdmxData rejection of a multi-entity variable is diagnosed",
			req:           &V2GetVariableMetadataRequest{VariableDcids: []string{"Amount_ODA"}, EntityDcids: []string{"country/USA"}},
			sdmxErr:       status.Error(codes.InvalidArgument, "SDMX data query must include at least one observation property filter"),
			wantErr:       errV2WrongObservationProperty("Amount_ODA", []string{"observationAbout"}, []string{"donor", "recipient"}),
			wantSdmxCalls: 1,
		},
		{
			name:          "SdmxData unavailable",
			req:           &V2GetVariableMetadataRequest{VariableDcids: []string{"Count_Person"}, EntityDcids: []string{"country/USA"}},
			sdmxErr:       sdmxUnavailable,
			wantErr:       errV2ObservationsFailed(sdmxUnavailable),
			wantSdmxCalls: 1,
		},
		{
			// The lookups after SdmxData degrade on failure, so cancellation must not return a partial response.
			name:            "request cancelled after SdmxData",
			req:             &V2GetVariableMetadataRequest{VariableDcids: []string{"Count_Person"}, EntityDcids: []string{"country/USA"}},
			cancelAfterSdmx: true,
			wantErr:         errV2ObservationsFailed(context.Canceled),
			wantSdmxCalls:   1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockV2MetadataServer{
				mockV2ObservationsServer: &mockV2ObservationsServer{
					sdmxErr: tc.sdmxErr,
					nodes: map[string]mockV2ObsNode{
						"Amount_ODA": {types: []string{"StatisticalVariable"}, observationProperties: []string{"donor", "recipient"}},
						"geoId/06":   {types: []string{"State"}},
					},
				},
				graphs:    metadataGraphs(),
				samples:   tc.samples,
				sampleErr: tc.sampleErr,
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelAfterSdmx {
				mock.afterSdmx = cancel
			}
			_, err := NewService(mock, NewCache(mock), nil).V2GetVariableMetadata(ctx, tc.req)
			gotSt, wantSt := status.Convert(err), status.Convert(tc.wantErr)
			if gotSt.Code() != wantSt.Code() || gotSt.Message() != wantSt.Message() {
				t.Errorf("V2GetVariableMetadata() error = (%v, %q), want (%v, %q)", gotSt.Code(), gotSt.Message(), wantSt.Code(), wantSt.Message())
			}
			for _, backendErr := range []error{tc.sampleErr, tc.sdmxErr} {
				if backendErr == nil {
					continue
				}
				if detail := status.Convert(backendErr).Message(); strings.Contains(gotSt.Message(), detail) {
					t.Errorf("error message %q leaks backend detail %q", gotSt.Message(), detail)
				}
			}
			if len(mock.sampleReqs) != tc.wantSampleCalls {
				t.Errorf("SampleChildEntities calls = %d, want %d", len(mock.sampleReqs), tc.wantSampleCalls)
			}
			if len(mock.sdmxReqs) != tc.wantSdmxCalls {
				t.Errorf("SdmxData calls = %d, want %d", len(mock.sdmxReqs), tc.wantSdmxCalls)
			}
		})
	}
}
