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
	"path/filepath"
	"slices"
	"strings"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
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

func TestSDMXDataUsesRemoteContainedInPlaceWithLocalObservations(t *testing.T) {
	var nodeCalls int
	remoteSource := &sdmxRemoteDataSource{
		nodeFunc: func(_ context.Context, req *pbv2.NodeRequest, _ int) (*pbv2.NodeResponse, error) {
			nodeCalls++
			if got, want := req.GetNodes(), []string{"europe"}; !slices.Equal(got, want) {
				return nil, fmt.Errorf("Node() nodes = %v, want %v", got, want)
			}
			if got, want := req.GetProperty(), "<-containedInPlace+{typeOf:Country}"; got != want {
				return nil, fmt.Errorf("Node() property = %q, want %q", got, want)
			}
			return &pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"europe": {
						Arcs: map[string]*pbv2.Nodes{
							"containedInPlace+": {Nodes: []*pb.EntityInfo{{Dcid: "country/USA"}}},
						},
					},
				},
			}, nil
		},
	}
	sdmxService := newSDMXServiceWithRemote(requireSuite(t).spannerClient, remoteSource)
	response, err := sdmxService.Data(context.Background(), emulatorDataRequest(
		"c[variableMeasured]=Count_Migration&c[destinationCountry]=country%2FCAN&"+
			"c[sourceCountry.containedInPlace+]=europe&c[sourceCountry.typeOf]=Country",
	))
	if err != nil {
		t.Fatalf("Data() error = %v", err)
	}
	if nodeCalls != 1 {
		t.Fatalf("Node() calls = %d, want 1", nodeCalls)
	}
	compareEmulatorCSVGolden(t, "data_two_entities.csv", string(response.Body))
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
