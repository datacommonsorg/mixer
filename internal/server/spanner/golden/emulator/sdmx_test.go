// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	sdmxformat "github.com/datacommonsorg/mixer/internal/server/sdmx/format"
	service "github.com/datacommonsorg/mixer/internal/server/sdmx/service"
	mixerspanner "github.com/datacommonsorg/mixer/internal/server/spanner"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type sdmxRemoteDataSource struct {
	datasource.DataSource
	nodeFunc func(context.Context, *pbv2.NodeRequest, int) (*pbv2.NodeResponse, error)
}

func (s *sdmxRemoteDataSource) Type() datasource.DataSourceType { return datasource.TypeRemote }
func (s *sdmxRemoteDataSource) Id() string                      { return "sdmx-remote" }
func (s *sdmxRemoteDataSource) Node(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
	return s.nodeFunc(ctx, req, pageSize)
}
func (s *sdmxRemoteDataSource) SdmxData(context.Context, *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error) {
	return &sdmxpb.SdmxDataResult{}, nil
}
func (s *sdmxRemoteDataSource) SdmxAvailability(context.Context, *sdmxpb.SdmxAvailabilityQuery) (*sdmxpb.SdmxAvailabilityResult, error) {
	return &sdmxpb.SdmxAvailabilityResult{}, nil
}

func TestSDMXData(t *testing.T) {
	sdmxService := newSDMXService(requireSuite(t).spannerClient)
	tests := []struct {
		name   string
		query  string
		golden string
	}{
		{
			name:   "two observation properties",
			query:  "c[variableMeasured]=Count_Migration&c[sourceCountry]=country%2FUSA&c[destinationCountry]=country%2FCAN",
			golden: "data_two_entities.csv",
		},
		{
			name:   "fallback observationAbout",
			query:  "c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA",
			golden: "data_fallback_observation_about.csv",
		},
		{
			name:   "three observation properties",
			query:  "c[variableMeasured]=Count_MigrationByTransportMode&c[destinationCountry]=country%2FCAN&c[sourceCountry]=country%2FUSA&c[transportMode]=Air&c[unit]=Count&c[measurementMethod]=Census&c[observationPeriod]=P1Y&c[provenance]=dc%2Fbase%2FHumanReadableStatVars",
			golden: "data_three_entities.csv",
		},
		{
			name:   "explicit middle observationAbout",
			query:  "c[variableMeasured]=Count_MigrationByObservationAbout&c[destinationCountry]=country%2FCAN&c[observationAbout]=country%2FMEX&c[sourceCountry]=country%2FUSA",
			golden: "data_explicit_observation_about.csv",
		},
		{
			name:   "compatible two-property stat variables",
			query:  "c[variableMeasured]=Count_Migration,Count_Refugee&c[destinationCountry]=country%2FCAN,country%2FGBR,country%2FMEX",
			golden: "data_compatible_stat_vars.csv",
		},
		{
			name:   "compatible fallback stat variables",
			query:  "c[variableMeasured]=Count_Household,Count_Person&c[observationAbout]=geoId%2F06,country%2FUSA",
			golden: "data_fallback_compatible_stat_vars.csv",
		},
		{
			name: "multiple values for every dimension",
			query: "c[variableMeasured]=Count_Migration,Count_Refugee&" +
				"c[destinationCountry]=country%2FCAN,country%2FMEX&" +
				"c[sourceCountry]=country%2FUSA,country%2FIND&" +
				"c[unit]=Count,Percent&c[measurementMethod]=Census,Survey&" +
				"c[observationPeriod]=P1Y,P1M&" +
				"c[provenance]=dc%2Fbase%2FHumanReadableStatVars,dc%2Fbase%2FOther",
			golden: "data_multiple_values.csv",
		},
		{
			name:   "facet ID",
			query:  "c[variableMeasured]=Count_Migration&c[destinationCountry]=country%2FCAN,country%2FGBR&c[facetId]=facet",
			golden: "data_facet_id.csv",
		},
		{
			name:   "empty result",
			query:  "c[variableMeasured]=Count_Migration&c[destinationCountry]=country%2FZZZ",
			golden: "data_empty.csv",
		},
		{
			name:   "contained destination on entity1",
			query:  "c[variableMeasured]=Count_Migration&c[destinationCountry.containedInPlace+]=northamerica&c[destinationCountry.typeOf]=Country&c[sourceCountry]=country%2FUSA",
			golden: "data_two_entities.csv",
		},
		{
			name:   "contained source on entity2",
			query:  "c[variableMeasured]=Count_Migration&c[destinationCountry]=country%2FCAN&c[sourceCountry.containedInPlace+]=northamerica&c[sourceCountry.typeOf]=Country",
			golden: "data_two_entities.csv",
		},
		{
			name:   "same containment on entity2 and entity3",
			query:  "c[variableMeasured]=Count_MigrationByObservationAbout&c[observationAbout.containedInPlace+]=northamerica&c[observationAbout.typeOf]=Country&c[sourceCountry.containedInPlace+]=northamerica&c[sourceCountry.typeOf]=Country",
			golden: "data_explicit_observation_about.csv",
		},
		{
			name:   "empty containment result",
			query:  "c[variableMeasured]=Count_Migration&c[sourceCountry.containedInPlace+]=oceania&c[sourceCountry.typeOf]=Country",
			golden: "data_empty.csv",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			response, err := sdmxService.Data(context.Background(), emulatorDataRequest(testCase.query))
			if err != nil {
				t.Fatalf("Data() error = %v", err)
			}
			if response.ContentType != sdmxformat.CSVContentType {
				t.Fatalf("Data() content type = %q, want %q", response.ContentType, sdmxformat.CSVContentType)
			}
			compareEmulatorCSVGolden(t, testCase.golden, string(response.Body))
		})
	}
}

func TestSDMXDataTimePeriods(t *testing.T) {
	sdmxService := newSDMXService(requireSuite(t).spannerClient)
	paths := []struct {
		name     string
		selector string
	}{
		{
			name:     "direct",
			selector: "c[observationAbout]=country%2FUSA",
		},
		{
			name:     "contained",
			selector: "c[observationAbout.containedInPlace+]=northamerica&c[observationAbout.typeOf]=Country",
		},
	}
	timeSelections := []struct {
		name   string
		filter string
		golden string
	}{
		{
			name:   "all",
			golden: "data_time_period_all.csv",
		},
		{
			name:   "latest",
			filter: "LATEST",
			golden: "data_time_period_latest.csv",
		},
		{
			name:   "one_date",
			filter: "2020",
			golden: "data_time_period_single_date.csv",
		},
		{
			name:   "two_dates",
			filter: "2020,2022",
			golden: "data_time_period_two_dates.csv",
		},
		{
			name:   "ten_dates",
			filter: "2010,2011,2012,2013,2014,2015,2016,2020,2022,2023",
			golden: "data_time_period_date_list.csv",
		},
		{
			name:   "eleven_dates",
			filter: "2010,2011,2012,2013,2014,2015,2016,2017,2020,2022,2023",
			golden: "data_time_period_date_list.csv",
		},
	}

	for _, path := range paths {
		for _, selection := range timeSelections {
			t.Run(path.name+"/"+selection.name, func(t *testing.T) {
				t.Parallel()
				query := "c[variableMeasured]=Count_TimeSeries&" + path.selector
				if selection.filter != "" {
					query += "&c[TIME_PERIOD]=" + selection.filter
				}
				response, err := sdmxService.Data(context.Background(), emulatorDataRequest(query))
				if err != nil {
					t.Fatalf("Data() error = %v", err)
				}
				if response.ContentType != sdmxformat.CSVContentType {
					t.Fatalf("Data() content type = %q, want %q", response.ContentType, sdmxformat.CSVContentType)
				}
				compareEmulatorCSVGolden(t, selection.golden, string(response.Body))
			})
		}
	}
}

func TestSDMXDataUsesRemoteContainedInPlaceWithLocalObservations(t *testing.T) {
	earthCountries := datacommons.ContainedInPlaceConstraint{Ancestor: "Earth", ChildPlaceType: "Country"}
	northAmericaCountries := datacommons.ContainedInPlaceConstraint{Ancestor: "northamerica", ChildPlaceType: "Country"}
	europeCountries := datacommons.ContainedInPlaceConstraint{Ancestor: "europe", ChildPlaceType: "Country"}
	usaStates := datacommons.ContainedInPlaceConstraint{Ancestor: "country/USA", ChildPlaceType: "State"}
	expansions := map[datacommons.ContainedInPlaceConstraint][]string{
		earthCountries:        {"country/BLZ", "country/CAN", "country/FRA", "country/GBR", "country/IND", "country/MEX", "country/USA"},
		northAmericaCountries: {"country/BLZ", "country/CAN", "country/MEX", "country/USA"},
		europeCountries:       {"country/FRA", "country/GBR"},
		usaStates:             {"geoId/06"},
	}
	tests := []struct {
		name             string
		query            string
		golden           string
		wantRelations    []datacommons.ContainedInPlaceConstraint
		verifyLocalEmpty bool
	}{
		{
			name: "remote-only entity1 containment",
			query: "c[variableMeasured]=Count_Migration&" +
				"c[destinationCountry.containedInPlace+]=Earth&c[destinationCountry.typeOf]=Country&" +
				"c[sourceCountry]=country%2FUSA",
			golden:           "data_two_entities.csv",
			wantRelations:    []datacommons.ContainedInPlaceConstraint{earthCountries},
			verifyLocalEmpty: true,
		},
		{
			name: "remote-only entity2 containment",
			query: "c[variableMeasured]=Count_Migration&c[destinationCountry]=country%2FCAN&" +
				"c[sourceCountry.containedInPlace+]=Earth&c[sourceCountry.typeOf]=Country",
			golden:        "data_two_entities.csv",
			wantRelations: []datacommons.ContainedInPlaceConstraint{earthCountries},
		},
		{
			name: "remote-only entity3 containment",
			query: "c[variableMeasured]=Count_MigrationByObservationAbout&" +
				"c[destinationCountry]=country%2FCAN&c[observationAbout]=country%2FMEX&" +
				"c[sourceCountry.containedInPlace+]=Earth&c[sourceCountry.typeOf]=Country",
			golden:        "data_explicit_observation_about.csv",
			wantRelations: []datacommons.ContainedInPlaceConstraint{earthCountries},
		},
		{
			name: "distinct destination and source relations",
			query: "c[variableMeasured]=Count_Migration&" +
				"c[destinationCountry.containedInPlace+]=northamerica&c[destinationCountry.typeOf]=Country&" +
				"c[sourceCountry.containedInPlace+]=Earth&c[sourceCountry.typeOf]=Country",
			golden:        "data_two_entities.csv",
			wantRelations: []datacommons.ContainedInPlaceConstraint{earthCountries, northAmericaCountries},
		},
		{
			name: "shared relation across components and variables",
			query: "c[variableMeasured]=Count_Migration,Count_Refugee&" +
				"c[destinationCountry.containedInPlace+]=Earth&c[destinationCountry.typeOf]=Country&" +
				"c[sourceCountry.containedInPlace+]=Earth&c[sourceCountry.typeOf]=Country",
			golden:        "data_compatible_stat_vars.csv",
			wantRelations: []datacommons.ContainedInPlaceConstraint{earthCountries},
		},
		{
			name: "mixed relations across three entity slots",
			query: "c[variableMeasured]=Count_MigrationByObservationAbout&" +
				"c[destinationCountry.containedInPlace+]=northamerica&c[destinationCountry.typeOf]=Country&" +
				"c[observationAbout.containedInPlace+]=Earth&c[observationAbout.typeOf]=Country&" +
				"c[sourceCountry.containedInPlace+]=Earth&c[sourceCountry.typeOf]=Country",
			golden:        "data_explicit_observation_about.csv",
			wantRelations: []datacommons.ContainedInPlaceConstraint{earthCountries, northAmericaCountries},
		},
		{
			name: "state containment",
			query: "c[variableMeasured]=Count_Household&" +
				"c[observationAbout.containedInPlace+]=country%2FUSA&c[observationAbout.typeOf]=State",
			golden:        "data_remote_containment_state.csv",
			wantRelations: []datacommons.ContainedInPlaceConstraint{usaStates},
		},
		{
			name: "local and remote containment union",
			query: "c[variableMeasured]=Count_RemoteMigration&" +
				"c[destinationCountry.containedInPlace+]=northamerica&c[destinationCountry.typeOf]=Country&" +
				"c[sourceCountry]=country%2FUSA",
			golden:        "data_remote_containment_union.csv",
			wantRelations: []datacommons.ContainedInPlaceConstraint{northAmericaCountries},
		},
		{
			name: "empty intersection of distinct relations",
			query: "c[variableMeasured]=Count_Migration&" +
				"c[destinationCountry.containedInPlace+]=europe&c[destinationCountry.typeOf]=Country&" +
				"c[sourceCountry.containedInPlace+]=northamerica&c[sourceCountry.typeOf]=Country",
			golden:        "data_empty.csv",
			wantRelations: []datacommons.ContainedInPlaceConstraint{europeCountries, northAmericaCountries},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.verifyLocalEmpty {
				response, err := newSDMXService(requireSuite(t).spannerClient).Data(
					context.Background(), emulatorDataRequest(testCase.query),
				)
				if err != nil {
					t.Fatalf("local Data() error = %v", err)
				}
				compareEmulatorCSVGolden(t, "data_empty.csv", string(response.Body))
			}

			var mu sync.Mutex
			calls := map[datacommons.ContainedInPlaceConstraint]int{}
			var requestErrors []string
			remoteSource := &sdmxRemoteDataSource{
				nodeFunc: func(_ context.Context, req *pbv2.NodeRequest, _ int) (*pbv2.NodeResponse, error) {
					relation, err := matchSdmxRemoteExpansion(req, expansions)
					if err != nil {
						mu.Lock()
						requestErrors = append(requestErrors, err.Error())
						mu.Unlock()
						return nil, err
					}
					mu.Lock()
					calls[relation]++
					mu.Unlock()
					return sdmxRemoteNodeResponse(relation, expansions[relation]), nil
				},
			}
			sdmxService := newSDMXServiceWithRemote(requireSuite(t).spannerClient, remoteSource)
			response, err := sdmxService.Data(context.Background(), emulatorDataRequest(testCase.query))
			if err != nil {
				t.Fatalf("Data() error = %v", err)
			}

			mu.Lock()
			gotCalls := maps.Clone(calls)
			gotRequestErrors := slices.Clone(requestErrors)
			mu.Unlock()
			if len(gotRequestErrors) > 0 {
				t.Fatalf("unexpected remote Node requests: %v", gotRequestErrors)
			}
			wantCalls := map[datacommons.ContainedInPlaceConstraint]int{}
			for _, relation := range testCase.wantRelations {
				wantCalls[relation] = 1
			}
			if diff := cmp.Diff(wantCalls, gotCalls); diff != "" {
				t.Fatalf("Node() calls mismatch (-want +got):\n%s", diff)
			}
			if response.ContentType != sdmxformat.CSVContentType {
				t.Fatalf("Data() content type = %q, want %q", response.ContentType, sdmxformat.CSVContentType)
			}
			compareEmulatorCSVGolden(t, testCase.golden, string(response.Body))
		})
	}
}

func matchSdmxRemoteExpansion(
	req *pbv2.NodeRequest,
	expansions map[datacommons.ContainedInPlaceConstraint][]string,
) (datacommons.ContainedInPlaceConstraint, error) {
	for relation := range expansions {
		wantProperty := fmt.Sprintf("<-containedInPlace+{typeOf:%s}", relation.ChildPlaceType)
		if slices.Equal(req.GetNodes(), []string{relation.Ancestor}) && req.GetProperty() == wantProperty {
			return relation, nil
		}
	}
	return datacommons.ContainedInPlaceConstraint{}, fmt.Errorf(
		"Node() request = nodes %v, property %q; no expansion configured",
		req.GetNodes(),
		req.GetProperty(),
	)
}

func sdmxRemoteNodeResponse(
	relation datacommons.ContainedInPlaceConstraint,
	dcids []string,
) *pbv2.NodeResponse {
	nodes := make([]*pb.EntityInfo, 0, len(dcids))
	for _, dcid := range dcids {
		nodes = append(nodes, &pb.EntityInfo{Dcid: dcid})
	}
	return &pbv2.NodeResponse{
		Data: map[string]*pbv2.LinkedGraph{
			relation.Ancestor: {
				Arcs: map[string]*pbv2.Nodes{
					"containedInPlace+": {Nodes: nodes},
				},
			},
		},
	}
}

func TestSDMXDataRequiresObservationProperty(t *testing.T) {
	sdmxService := newSDMXService(requireSuite(t).spannerClient)
	_, err := sdmxService.Data(context.Background(), emulatorDataRequest(
		"c[variableMeasured]=Count_Migration&c[facetId]=facet",
	))
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Data() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
	}
	want := "SDMX data query must include at least one observation property filter; allowed observation properties are [destinationCountry sourceCountry]"
	if got := status.Convert(err).Message(); got != want {
		t.Fatalf("Data() message = %q, want %q", got, want)
	}
}

func TestSDMXAvailability(t *testing.T) {
	sdmxService := newSDMXService(requireSuite(t).spannerClient)
	tests := []struct {
		name      string
		component string
		query     string
		golden    string
	}{
		{
			name:      "third observation property",
			component: "transportMode",
			query:     "c[variableMeasured]=Count_MigrationByTransportMode&c[destinationCountry]=country%2FCAN&c[sourceCountry]=country%2FUSA",
			golden:    "availability_transport_mode.json",
		},
		{
			name:      "explicit middle observationAbout",
			component: "observationAbout",
			query:     "c[variableMeasured]=Count_MigrationByObservationAbout&c[destinationCountry]=country%2FCAN&c[sourceCountry]=country%2FUSA",
			golden:    "availability_explicit_observation_about.json",
		},
		{
			name:      "fixed dimension across stat variables",
			component: "unit",
			query: "c[variableMeasured]=Count_Migration,Count_Refugee&" +
				"c[destinationCountry]=country%2FCAN,country%2FMEX&" +
				"c[sourceCountry]=country%2FUSA,country%2FIND",
			golden: "availability_unit.json",
		},
		{
			name:      "dynamic observation property across stat variables",
			component: "destinationCountry",
			query: "c[variableMeasured]=Count_Migration,Count_Refugee&" +
				"c[sourceCountry]=country%2FUSA,country%2FIND",
			golden: "availability_destination_country.json",
		},
		{
			name:      "fallback observationAbout for one stat variable",
			component: "observationAbout",
			query:     "c[variableMeasured]=Count_Person",
			golden:    "availability_fallback_observation_about.json",
		},
		{
			name:      "fallback observationAbout across stat variables",
			component: "observationAbout",
			query:     "c[variableMeasured]=Count_Household,Count_Person",
			golden:    "availability_fallback_compatible_stat_vars.json",
		},
		{
			name:      "variableMeasured across stat variables",
			component: "variableMeasured",
			query:     "c[variableMeasured]=Count_Migration,Count_Refugee",
			golden:    "availability_variable_measured.json",
		},
		{
			name:      "unmatched time period",
			component: "measurementMethod",
			query:     "c[variableMeasured]=Count_TimeSeries&c[TIME_PERIOD]=1999",
			golden:    "availability_empty.json",
		},
		{
			name:      "empty result",
			component: "sourceCountry",
			query:     "c[variableMeasured]=Count_Migration&c[destinationCountry]=country%2FZZZ",
			golden:    "availability_empty.json",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			response, err := sdmxService.Availability(context.Background(), emulatorAvailabilityRequest(testCase.component, testCase.query))
			if err != nil {
				t.Fatalf("Availability() error = %v", err)
			}
			if response.ContentType != sdmxformat.StructureJSONType {
				t.Fatalf("Availability() content type = %q, want %q", response.ContentType, sdmxformat.StructureJSONType)
			}
			compareEmulatorJSONGolden(t, testCase.golden, string(response.Body))
		})
	}
}

func TestSDMXAvailabilityContainedInPlace(t *testing.T) {
	sdmxService := newSDMXService(requireSuite(t).spannerClient)
	baseQuery := "c[variableMeasured]=Count_Migration,Count_Refugee&" +
		"c[sourceCountry.containedInPlace+]=northamerica&c[sourceCountry.typeOf]=Country"
	tests := []struct {
		name      string
		component string
		query     string
		golden    string
	}{
		{
			name:      "nonempty entity2",
			component: "destinationCountry",
			query:     baseQuery,
			golden:    "availability_contained_source_country.json",
		},
		{
			name:      "nonempty entity2 with time periods",
			component: "destinationCountry",
			query:     baseQuery + "&c[TIME_PERIOD]=2023,2024",
			golden:    "availability_contained_source_country_with_time_periods.json",
		},
		{
			name:      "empty entity2",
			component: "destinationCountry",
			query:     "c[variableMeasured]=Count_Migration&c[sourceCountry.containedInPlace+]=oceania&c[sourceCountry.typeOf]=Country",
			golden:    "availability_contained_empty.json",
		},
	}
	for _, testCase := range tests {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			response, err := sdmxService.Availability(
				context.Background(),
				emulatorAvailabilityRequest(testCase.component, testCase.query),
			)
			if err != nil {
				t.Fatalf("Availability() error = %v", err)
			}
			if response.ContentType != sdmxformat.StructureJSONType {
				t.Fatalf("Availability() content type = %q, want %q", response.ContentType, sdmxformat.StructureJSONType)
			}
			compareEmulatorJSONGolden(t, testCase.golden, string(response.Body))
		})
	}
}

func TestSDMXAvailabilityTimePeriods(t *testing.T) {
	sdmxService := newSDMXService(requireSuite(t).spannerClient)
	paths := []struct {
		name       string
		constraint string
	}{
		{name: "broad"},
		{
			name:       "filtered",
			constraint: "&c[unit]=Count",
		},
	}
	timeSelections := []struct {
		name     string
		filter   string
		golden   string
		wantCode codes.Code
	}{
		{
			name:   "all",
			golden: "availability_time_period_date_list.json",
		},
		{
			name:     "latest",
			filter:   "LATEST",
			wantCode: codes.InvalidArgument,
		},
		{
			name:   "one_date",
			filter: "2020",
			golden: "availability_time_period_single_date.json",
		},
		{
			name:   "two_dates",
			filter: "2020,2023",
			golden: "availability_time_period_date_list.json",
		},
		{
			name:   "ten_dates",
			filter: "2010,2011,2012,2013,2014,2015,2016,2017,2020,2023",
			golden: "availability_time_period_date_list.json",
		},
		{
			name:   "eleven_dates",
			filter: "2010,2011,2012,2013,2014,2015,2016,2017,2018,2020,2023",
			golden: "availability_time_period_date_list.json",
		},
	}

	for _, path := range paths {
		for _, selection := range timeSelections {
			t.Run(path.name+"/"+selection.name, func(t *testing.T) {
				t.Parallel()
				query := "c[variableMeasured]=Count_TimeSeries" + path.constraint
				if selection.filter != "" {
					query += "&c[TIME_PERIOD]=" + selection.filter
				}
				response, err := sdmxService.Availability(
					context.Background(),
					emulatorAvailabilityRequest("measurementMethod", query),
				)
				if status.Code(err) != selection.wantCode {
					t.Fatalf("Availability() code = %v, want %v; err = %v", status.Code(err), selection.wantCode, err)
				}
				if selection.wantCode != codes.OK {
					if !strings.Contains(status.Convert(err).Message(), "LATEST is not valid for availability") {
						t.Fatalf("Availability() message = %q, want LATEST validation error", status.Convert(err).Message())
					}
					return
				}
				if response.ContentType != sdmxformat.StructureJSONType {
					t.Fatalf("Availability() content type = %q, want %q", response.ContentType, sdmxformat.StructureJSONType)
				}
				compareEmulatorJSONGolden(t, selection.golden, string(response.Body))
			})
		}
	}
}

func TestSDMXAvailabilityUsesRemoteContainedInPlaceWithLocalObservations(t *testing.T) {
	relation := datacommons.ContainedInPlaceConstraint{Ancestor: "Earth", ChildPlaceType: "Country"}
	expansions := map[datacommons.ContainedInPlaceConstraint][]string{
		relation: {"country/USA"},
	}
	var calls int
	remoteSource := &sdmxRemoteDataSource{
		nodeFunc: func(_ context.Context, req *pbv2.NodeRequest, _ int) (*pbv2.NodeResponse, error) {
			got, err := matchSdmxRemoteExpansion(req, expansions)
			if err != nil {
				return nil, err
			}
			calls++
			return sdmxRemoteNodeResponse(got, expansions[got]), nil
		},
	}
	sdmxService := newSDMXServiceWithRemote(requireSuite(t).spannerClient, remoteSource)
	response, err := sdmxService.Availability(context.Background(), emulatorAvailabilityRequest(
		"destinationCountry",
		"c[variableMeasured]=Count_Migration,Count_Refugee&c[sourceCountry.containedInPlace+]=Earth&c[sourceCountry.typeOf]=Country",
	))
	if err != nil {
		t.Fatalf("Availability() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("Node() calls = %d, want 1", calls)
	}
	if response.ContentType != sdmxformat.StructureJSONType {
		t.Fatalf("Availability() content type = %q, want %q", response.ContentType, sdmxformat.StructureJSONType)
	}
	compareEmulatorJSONGolden(t, "availability_remote_contained_source_country.json", string(response.Body))
}

func TestSDMXRejectsIncompatibleStatVariableShapes(t *testing.T) {
	sdmxService := newSDMXService(requireSuite(t).spannerClient)
	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "data",
			call: func() error {
				_, err := sdmxService.Data(context.Background(), emulatorDataRequest(
					"c[variableMeasured]=Count_Migration,Count_Person",
				))
				return err
			},
		},
		{
			name: "availability",
			call: func() error {
				_, err := sdmxService.Availability(context.Background(), emulatorAvailabilityRequest(
					"destinationCountry", "c[variableMeasured]=Count_Migration,Count_Person",
				))
				return err
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.call()
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("SDMX request code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
			}
			if !strings.Contains(status.Convert(err).Message(), "incompatible observationProperties") {
				t.Fatalf("SDMX request message = %q, want incompatible observationProperties", status.Convert(err).Message())
			}
		})
	}
}

func newSDMXService(client mixerspanner.SpannerClient) *service.Service {
	spannerSource := mixerspanner.NewSpannerDataSource(client, nil)
	sources := datasources.NewDataSources([]datasource.DataSource{spannerSource}, nil)
	return service.New(dispatcher.NewDispatcher(nil, sources))
}

func newSDMXServiceWithRemote(
	client mixerspanner.SpannerClient,
	remoteSource datasource.DataSource,
) *service.Service {
	spannerSource := mixerspanner.NewSpannerDataSource(client, nil)
	sources := datasources.NewDataSources([]datasource.DataSource{spannerSource, remoteSource}, remoteSource)
	var relationProcessor dispatcher.Processor = dispatcher.NewRelationExpressionProcessor(remoteSource, 10000)
	return service.New(dispatcher.NewDispatcher([]*dispatcher.Processor{&relationProcessor}, sources))
}

func emulatorDataRequest(query string) service.Request {
	tail := "dataflow/DC/DF_OBS/1.0.0/*"
	return service.Request{Tail: tail, OriginalURI: "/sdmx/v3/data/" + tail + "?" + query}
}

func emulatorAvailabilityRequest(component, query string) service.Request {
	tail := "dataflow/DC/DF_OBS/1.0.0/*/" + component
	return service.Request{Tail: tail, OriginalURI: "/sdmx/v3/availability/" + tail + "?" + query}
}

func compareEmulatorCSVGolden(t *testing.T, filename, actual string) {
	t.Helper()
	compareEmulatorGolden(t, filename, actual, normalizeCSV)
}

func compareEmulatorJSONGolden(t *testing.T, filename, actual string) {
	t.Helper()
	compareEmulatorGolden(t, filename, actual, normalizedJSON)
}

func compareEmulatorGolden(t *testing.T, filename, actual string, normalize func(*testing.T, string) string) {
	t.Helper()
	directory := filepath.Join("testdata", "sdmx")
	actual = normalize(t, actual)
	if test.GenerateGolden {
		if err := test.WriteGolden(actual, directory, filename); err != nil {
			t.Fatalf("WriteGolden(%q) error = %v", filename, err)
		}
		return
	}
	expected, err := test.ReadGolden(directory, filename)
	if err != nil {
		t.Fatalf("ReadGolden(%q) error = %v", filename, err)
	}
	expected = normalize(t, expected)
	if diff := cmp.Diff(expected, actual); diff != "" {
		t.Errorf("response mismatch (-want +got):\n%s", diff)
	}
}

func normalizeCSV(_ *testing.T, value string) string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	hasTrailingNewline := strings.HasSuffix(value, "\n")
	lines := strings.Split(strings.TrimSuffix(value, "\n"), "\n")
	// Spanner does not guarantee time-series order, so compare CSV data rows independently of order.
	if len(lines) > 1 {
		slices.Sort(lines[1:])
	}
	value = strings.Join(lines, "\n")
	if hasTrailingNewline {
		value += "\n"
	}
	return value
}

func normalizedJSON(t *testing.T, value string) string {
	t.Helper()
	var decoded interface{}
	if err := json.Unmarshal([]byte(value), &decoded); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}
	formatted, err := json.MarshalIndent(decoded, "", "  ")
	if err != nil {
		t.Fatalf("format JSON response: %v", err)
	}
	return string(formatted)
}
