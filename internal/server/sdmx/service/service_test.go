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

package service

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	sdmxformat "github.com/datacommonsorg/mixer/internal/server/sdmx/format"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

func sdmxComponentConstraint(values ...string) *sdmxpb.SdmxComponentConstraint {
	predicates := make([]*sdmxpb.SdmxPredicate, 0, len(values))
	for _, value := range values {
		predicates = append(predicates, &sdmxpb.SdmxPredicate{Value: value})
	}
	return &sdmxpb.SdmxComponentConstraint{Predicates: predicates}
}

func TestConstraintsFromRESTFiltersDefaultsToEquality(t *testing.T) {
	constraint := constraintsFromRESTFilters(map[string][]string{
		"source": {"india", "usa"},
	})["source"]

	gotValues := make([]string, 0, len(constraint.GetPredicates()))
	for _, predicate := range constraint.GetPredicates() {
		if got := predicate.GetOperator(); got != sdmxpb.SdmxOperator_SDMX_OPERATOR_EQ {
			t.Fatalf("predicate operator = %v, want EQ", got)
		}
		gotValues = append(gotValues, predicate.GetValue())
	}
	if diff := cmp.Diff([]string{"india", "usa"}, gotValues); diff != "" {
		t.Fatalf("predicate values mismatch (-want +got):\n%s", diff)
	}
}

type sdmxDataSource struct {
	datasource.DataSource
	result             *sdmxpb.SdmxDataResult
	err                error
	got                *sdmxpb.SdmxDataQuery
	availabilityResult *sdmxpb.SdmxAvailabilityResult
	availabilityErr    error
	gotAvailability    *sdmxpb.SdmxAvailabilityQuery
}

func (ds *sdmxDataSource) Type() datasource.DataSourceType {
	return datasource.TypeMock
}

func (ds *sdmxDataSource) Id() string {
	return "sdmx-test"
}

func (ds *sdmxDataSource) SdmxData(ctx context.Context, req *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error) {
	ds.got = req
	return ds.result, ds.err
}

func (ds *sdmxDataSource) SdmxAvailability(ctx context.Context, req *sdmxpb.SdmxAvailabilityQuery) (*sdmxpb.SdmxAvailabilityResult, error) {
	ds.gotAvailability = req
	return ds.availabilityResult, ds.availabilityErr
}

func testSdmxDataResult(observationProperties []string, series []*sdmxpb.SdmxTimeSeries) *sdmxpb.SdmxDataResult {
	components := datacommons.DataComponentsForObservationProperties(observationProperties)
	result := &sdmxpb.SdmxDataResult{
		Shape: &sdmxpb.SdmxDataShape{
			Components: make([]*sdmxpb.SdmxComponent, 0, len(components)),
		},
		Series: series,
	}
	for _, component := range components {
		result.Shape.Components = append(result.Shape.Components, &sdmxpb.SdmxComponent{
			Id:   component.ID,
			Kind: testProtoComponentKind(component.Kind),
		})
	}
	return result
}

func testProtoComponentKind(kind datacommons.ComponentKind) sdmxpb.SdmxComponentKind {
	switch kind {
	case datacommons.ComponentKindDimension:
		return sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_DIMENSION
	case datacommons.ComponentKindMeasure:
		return sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_MEASURE
	case datacommons.ComponentKindAttribute:
		return sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_ATTRIBUTE
	default:
		return sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_UNSPECIFIED
	}
}

func TestDataValidation(t *testing.T) {
	tests := []struct {
		name       string
		request    Request
		wantCode   codes.Code
		wantErrSub string
	}{
		{
			name:       "Missing variable measured",
			request:    sdmxDataRequest("c[observationAbout]=country%2FUSA"),
			wantCode:   codes.InvalidArgument,
			wantErrSub: "missing required SDMX component filter variableMeasured",
		},
		{
			name:       "Unsupported AND filter",
			request:    sdmxDataRequest("c[variableMeasured]=Count+Person&c[observationAbout]=country%2FUSA"),
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX AND filters are not implemented yet",
		},
		{
			name:       "Unsupported operator",
			request:    sdmxDataRequest("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA&c[TIME_PERIOD]=ge:2020"),
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX component filter operators are not implemented yet",
		},
		{
			name:       "Explicit equality remains unsupported",
			request:    sdmxDataRequest("c[variableMeasured]=Count_Person&c[observationAbout]=eq:country%2FUSA"),
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX component filter operators are not implemented yet",
		},
		{
			name:       "Property selector remains unsupported",
			request:    sdmxDataRequest("c[variableMeasured]=Count_Person&c[observationAbout.typeOf]=County"),
			wantCode:   codes.InvalidArgument,
			wantErrSub: "invalid SDMX component filter",
		},
		{
			name:       "Unsupported observation value filter",
			request:    sdmxDataRequest("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA&c[OBS_VALUE]=10"),
			wantCode:   codes.Unimplemented,
			wantErrSub: "unsupported SDMX component filter",
		},
		{
			name:       "Unsupported time period filter",
			request:    sdmxDataRequest("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA&c[TIME_PERIOD]=2020"),
			wantCode:   codes.Unimplemented,
			wantErrSub: "unsupported SDMX component filter",
		},
		{
			name:       "Unsupported scaling factor filter",
			request:    sdmxDataRequest("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA&c[scalingFactor]=0"),
			wantCode:   codes.Unimplemented,
			wantErrSub: "unsupported SDMX component filter",
		},
		{
			name: "Unsupported non star key",
			request: Request{
				Tail:        "dataflow/DC/DF_OBS/1.0.0/A.US",
				OriginalURI: "/sdmx/v3/data/dataflow/DC/DF_OBS/1.0.0/A.US?c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA",
			},
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX data keys other than * are not implemented yet",
		},
		{
			name:       "Unsupported SDMX JSON media type",
			request:    withAccept(sdmxDataRequest("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"), "application/vnd.sdmx.data+json;version=2.0.0"),
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX JSON responses are not implemented yet",
		},
		{
			name:       "Unsupported SDMX CSV option",
			request:    withAccept(sdmxDataRequest("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"), "application/vnd.sdmx.data+csv;version=2.0.0;labels=name"),
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX CSV response option",
		},
	}

	svc := newSdmxTestService(&sdmxDataSource{})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := svc.Data(context.Background(), tt.request)
			if err == nil {
				t.Fatal("Data() error = nil, want error")
			}
			if status.Code(err) != tt.wantCode {
				t.Errorf("Data() code = %v, want %v; err = %v", status.Code(err), tt.wantCode, err)
			}
			if !strings.Contains(status.Convert(err).Message(), tt.wantErrSub) {
				t.Errorf("Data() message = %q, want substring %q", status.Convert(err).Message(), tt.wantErrSub)
			}
		})
	}
}

func TestDataSuccess(t *testing.T) {
	ds := &sdmxDataSource{
		result: testSdmxDataResult([]string{datacommons.ComponentObservationAbout}, []*sdmxpb.SdmxTimeSeries{
			{
				Dimensions: map[string]string{
					datacommons.ComponentVariableMeasured: "Count_Person",
					datacommons.ComponentObservationAbout: "country/USA",
					datacommons.ComponentProvenance:       "dc/base",
				},
				Points: []*sdmxpb.SdmxDataPoint{
					{TimePeriod: "2020", ObservationValue: "1"},
				},
			},
		}),
	}
	svc := newSdmxTestService(ds)

	response, err := svc.Data(context.Background(), sdmxDataRequest("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"))
	if err != nil {
		t.Fatalf("Data() error = %v", err)
	}

	wantQuery := &sdmxpb.SdmxDataQuery{
		Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("Count_Person"),
			"observationAbout": sdmxComponentConstraint("country/USA"),
		},
	}
	if diff := cmp.Diff(wantQuery, ds.got, protocmp.Transform()); diff != "" {
		t.Errorf("SdmxData query mismatch (-want +got):\n%s", diff)
	}
	if response.ContentType != sdmxformat.CSVContentType {
		t.Errorf("ContentType = %q, want %q", response.ContentType, sdmxformat.CSVContentType)
	}
	if !strings.HasPrefix(string(response.Body), "STRUCTURE,STRUCTURE_ID,ACTION,variableMeasured,observationAbout") {
		t.Errorf("response does not look like SDMX CSV: %s", response.Body)
	}
}

func TestDataDimensionFiltersPreserveMultipleValues(t *testing.T) {
	ds := &sdmxDataSource{
		result: testSdmxDataResult([]string{"destinationCountry", "sourceCountry"}, []*sdmxpb.SdmxTimeSeries{
			{
				Dimensions: map[string]string{
					datacommons.ComponentVariableMeasured: "Count_Person_Migrated",
					"destinationCountry":                  "country/CAN",
					"sourceCountry":                       "country/USA",
					datacommons.ComponentProvenance:       "dc/base",
				},
				Points: []*sdmxpb.SdmxDataPoint{
					{TimePeriod: "2020", ObservationValue: "1"},
				},
			},
		}),
	}
	svc := newSdmxTestService(ds)

	_, err := svc.Data(context.Background(), sdmxDataRequest(
		"c[variableMeasured]=Count_Person_Migrated,Count_Refugee&"+
			"c[destinationCountry]=country%2FCAN,country%2FMEX&"+
			"c[sourceCountry]=country%2FUSA,country%2FIND&"+
			"c[unit]=Person,Traveler&"+
			"c[measurementMethod]=Census,Survey&"+
			"c[observationPeriod]=P1Y,P1M&"+
			"c[provenance]=dc%2Fbase%2Fone,dc%2Fbase%2Ftwo",
	))
	if err != nil {
		t.Fatalf("Data() error = %v", err)
	}

	wantQuery := &sdmxpb.SdmxDataQuery{
		Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured":   sdmxComponentConstraint("Count_Person_Migrated", "Count_Refugee"),
			"destinationCountry": sdmxComponentConstraint("country/CAN", "country/MEX"),
			"sourceCountry":      sdmxComponentConstraint("country/USA", "country/IND"),
			"unit":               sdmxComponentConstraint("Person", "Traveler"),
			"measurementMethod":  sdmxComponentConstraint("Census", "Survey"),
			"observationPeriod":  sdmxComponentConstraint("P1Y", "P1M"),
			"provenance":         sdmxComponentConstraint("dc/base/one", "dc/base/two"),
		},
	}
	if diff := cmp.Diff(wantQuery, ds.got, protocmp.Transform()); diff != "" {
		t.Errorf("SdmxData query mismatch (-want +got):\n%s", diff)
	}
}

func TestDataCSVSuccess(t *testing.T) {
	ds := &sdmxDataSource{
		result: testSdmxDataResult([]string{datacommons.ComponentObservationAbout}, []*sdmxpb.SdmxTimeSeries{
			{
				Dimensions: map[string]string{
					datacommons.ComponentVariableMeasured:  "Count_Person",
					datacommons.ComponentObservationAbout:  "country/USA",
					datacommons.ComponentUnit:              "Person",
					datacommons.ComponentMeasurementMethod: "Census",
					datacommons.ComponentObservationPeriod: "P1Y",
					datacommons.ComponentProvenance:        "dc/base",
				},
				Attributes: map[string]string{
					datacommons.ComponentScalingFactor: "0",
					datacommons.ComponentFacetID:       "stored-facet-id",
				},
				Points: []*sdmxpb.SdmxDataPoint{
					{TimePeriod: "2020", ObservationValue: "1.50"},
				},
			},
		}),
	}
	svc := newSdmxTestService(ds)

	response, err := svc.Data(context.Background(), withAccept(
		sdmxDataRequest("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"),
		"application/vnd.sdmx.data+csv;version=2.0.0",
	))
	if err != nil {
		t.Fatalf("Data() error = %v", err)
	}
	if response.ContentType != sdmxformat.CSVContentType {
		t.Errorf("ContentType = %q, want %q", response.ContentType, sdmxformat.CSVContentType)
	}

	want := "STRUCTURE,STRUCTURE_ID,ACTION,variableMeasured,observationAbout,unit,measurementMethod,observationPeriod,provenance,TIME_PERIOD,OBS_VALUE,scalingFactor,facetId\r\n" +
		"dataflow,DC:DF_OBS(1.0.0),I,Count_Person,country/USA,Person,Census,P1Y,dc/base,2020,1.50,0,stored-facet-id\r\n"
	if got := string(response.Body); got != want {
		t.Errorf("Response body = %q, want %q", got, want)
	}
}

func TestDataFormatQueryOverridesAccept(t *testing.T) {
	ds := &sdmxDataSource{result: testSdmxDataResult([]string{datacommons.ComponentObservationAbout}, nil)}
	svc := newSdmxTestService(ds)

	response, err := svc.Data(context.Background(), withAccept(
		sdmxDataRequest("format=csv&c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"),
		"application/vnd.sdmx.data+json;version=2.0.0",
	))
	if err != nil {
		t.Fatalf("Data() error = %v", err)
	}
	if response.ContentType != sdmxformat.CSVContentType {
		t.Errorf("ContentType = %q, want %q", response.ContentType, sdmxformat.CSVContentType)
	}
}

func TestDataEmptyResult(t *testing.T) {
	ds := &sdmxDataSource{result: testSdmxDataResult([]string{datacommons.ComponentObservationAbout}, nil)}
	svc := newSdmxTestService(ds)

	response, err := svc.Data(context.Background(), sdmxDataRequest("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"))
	if err != nil {
		t.Fatalf("Data() error = %v", err)
	}
	body := string(response.Body)
	want := "STRUCTURE,STRUCTURE_ID,ACTION,variableMeasured,observationAbout,unit,measurementMethod,observationPeriod,provenance,TIME_PERIOD,OBS_VALUE,scalingFactor,facetId\r\n"
	if body != want {
		t.Errorf("Response body = %q, want %q", body, want)
	}
}

func TestDataCSVEmptyResult(t *testing.T) {
	ds := &sdmxDataSource{result: testSdmxDataResult([]string{datacommons.ComponentObservationAbout}, nil)}
	svc := newSdmxTestService(ds)

	response, err := svc.Data(context.Background(), withAccept(
		sdmxDataRequest("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"),
		"application/vnd.sdmx.data+csv;version=2.0.0",
	))
	if err != nil {
		t.Fatalf("Data() error = %v", err)
	}
	if got := string(response.Body); got != "STRUCTURE,STRUCTURE_ID,ACTION,variableMeasured,observationAbout,unit,measurementMethod,observationPeriod,provenance,TIME_PERIOD,OBS_VALUE,scalingFactor,facetId\r\n" {
		t.Errorf("Response body = %q, want header-only CSV", got)
	}
}

func TestDataMissingShape(t *testing.T) {
	ds := &sdmxDataSource{result: &sdmxpb.SdmxDataResult{}}
	svc := newSdmxTestService(ds)

	_, err := svc.Data(context.Background(), sdmxDataRequest("c[variableMeasured]=Count_Person"))
	if status.Code(err) != codes.Internal {
		t.Fatalf("Data() code = %v, want %v; err = %v", status.Code(err), codes.Internal, err)
	}
	if got, want := status.Convert(err).Message(), "Internal mapping error occurred."; got != want {
		t.Fatalf("Data() message = %q, want %q", got, want)
	}
}

func TestDataDispatcherError(t *testing.T) {
	ds := &sdmxDataSource{err: errors.New("dispatcher failed")}
	svc := newSdmxTestService(ds)

	_, err := svc.Data(context.Background(), sdmxDataRequest("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"))
	if status.Code(err) != codes.Internal {
		t.Fatalf("Data() code = %v, want %v; err = %v", status.Code(err), codes.Internal, err)
	}
}

func TestDataDispatcherStatusErrorPassesThrough(t *testing.T) {
	ds := &sdmxDataSource{err: status.Error(codes.InvalidArgument, "bad SDMX request")}
	svc := newSdmxTestService(ds)

	_, err := svc.Data(context.Background(), sdmxDataRequest("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"))
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Data() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
	}
	if got, want := status.Convert(err).Message(), "bad SDMX request"; got != want {
		t.Fatalf("Data() message = %q, want %q", got, want)
	}
}

func TestDataDispatcherStatusErrorDebugLogging(t *testing.T) {
	buf, restore := captureSdmxLogs()
	defer restore()

	ds := &sdmxDataSource{err: status.Error(codes.InvalidArgument, "bad SDMX request")}
	svc := newSdmxTestService(ds)

	_, err := svc.Data(context.Background(), withLog(sdmxDataRequest("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA")))
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Data() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
	}
	logs := buf.String()
	for _, want := range []string{
		"SDMX data dispatcher returned client error",
		"original_uri",
		"bad SDMX request",
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("logs do not contain %q: %s", want, logs)
		}
	}
}

func TestDataNilDispatcher(t *testing.T) {
	svc := New(nil)

	_, err := svc.Data(context.Background(), sdmxDataRequest("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"))
	if status.Code(err) != codes.Internal {
		t.Fatalf("Data() code = %v, want %v; err = %v", status.Code(err), codes.Internal, err)
	}
	if !strings.Contains(status.Convert(err).Message(), "Internal server error occurred while processing the request.") {
		t.Fatalf("Data() message = %q, want generic internal error", status.Convert(err).Message())
	}
}

func TestDataSDMXDebugLoggingDisabled(t *testing.T) {
	buf, restore := captureSdmxLogs()
	defer restore()

	ds := &sdmxDataSource{result: testSdmxDataResult([]string{datacommons.ComponentObservationAbout}, nil)}
	svc := newSdmxTestService(ds)

	_, err := svc.Data(context.Background(), sdmxDataRequest("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"))
	if err != nil {
		t.Fatalf("Data() error = %v", err)
	}
	logs := buf.String()
	if strings.Contains(logs, "SDMX data request parsed") || strings.Contains(logs, "SDMX data dispatcher request") {
		t.Fatalf("unexpected SDMX debug logs: %s", logs)
	}
}

func TestDataSDMXDebugLoggingSuccess(t *testing.T) {
	buf, restore := captureSdmxLogs()
	defer restore()

	ds := &sdmxDataSource{result: testSdmxDataResult([]string{datacommons.ComponentObservationAbout}, nil)}
	svc := newSdmxTestService(ds)

	_, err := svc.Data(context.Background(), withLog(sdmxDataRequest("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA")))
	if err != nil {
		t.Fatalf("Data() error = %v", err)
	}
	logs := buf.String()
	for _, want := range []string{
		"SDMX data request parsed",
		"original_uri",
		"Count_Person",
		"country/USA",
		"SDMX data dispatcher request",
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("logs do not contain %q: %s", want, logs)
		}
	}
}

func TestDataSDMXDebugLoggingParseFailure(t *testing.T) {
	buf, restore := captureSdmxLogs()
	defer restore()

	svc := newSdmxTestService(&sdmxDataSource{})

	_, err := svc.Data(context.Background(), withLog(sdmxDataRequest("c[observationAbout]=country%2FUSA")))
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Data() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
	}
	logs := buf.String()
	for _, want := range []string{
		"SDMX data request parse failed",
		"original_uri",
		"missing required SDMX component filter variableMeasured",
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("logs do not contain %q: %s", want, logs)
		}
	}
}

func TestAvailabilityValidation(t *testing.T) {
	tests := []struct {
		name       string
		request    Request
		wantCode   codes.Code
		wantErrSub string
	}{
		{
			name:       "Missing variable measured",
			request:    sdmxAvailabilityRequest("observationAbout", "mode=exact"),
			wantCode:   codes.InvalidArgument,
			wantErrSub: "missing required SDMX component filter variableMeasured",
		},
		{
			name:       "Unsupported selected component",
			request:    sdmxAvailabilityRequest("OBS_VALUE", "c[variableMeasured]=Count_Person"),
			wantCode:   codes.Unimplemented,
			wantErrSub: "unsupported SDMX availability component",
		},
		{
			name:       "Unsupported mode",
			request:    sdmxAvailabilityRequest("observationAbout", "mode=available&c[variableMeasured]=Count_Person"),
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX availability mode",
		},
		{
			name:       "Unsupported CSV accept",
			request:    withAccept(sdmxAvailabilityRequest("observationAbout", "c[variableMeasured]=Count_Person"), "application/vnd.sdmx.data+csv;version=2.0.0"),
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX availability response media type",
		},
	}

	svc := newSdmxTestService(&sdmxDataSource{})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := svc.Availability(context.Background(), tt.request)
			if err == nil {
				t.Fatal("Availability() error = nil, want error")
			}
			if status.Code(err) != tt.wantCode {
				t.Errorf("Availability() code = %v, want %v; err = %v", status.Code(err), tt.wantCode, err)
			}
			if !strings.Contains(status.Convert(err).Message(), tt.wantErrSub) {
				t.Errorf("Availability() message = %q, want substring %q", status.Convert(err).Message(), tt.wantErrSub)
			}
		})
	}
}

func TestAvailabilityBackendUnimplemented(t *testing.T) {
	ds := &sdmxDataSource{
		availabilityErr: status.Error(codes.Unimplemented, "SDMX availability backend is not implemented yet"),
	}
	svc := newSdmxTestService(ds)

	response, err := svc.Availability(context.Background(), sdmxAvailabilityRequest("observationAbout", "c[variableMeasured]=Count_Person"))
	if response != nil {
		t.Fatalf("Availability() response = %v, want nil", response)
	}
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("Availability() code = %v, want %v; err = %v", status.Code(err), codes.Unimplemented, err)
	}
	if !strings.Contains(status.Convert(err).Message(), "SDMX availability backend is not implemented yet") {
		t.Fatalf("Availability() message = %q, want backend unimplemented", status.Convert(err).Message())
	}
}

func TestAvailabilityBackendInvalidArgument(t *testing.T) {
	ds := &sdmxDataSource{
		availabilityErr: status.Error(codes.InvalidArgument, "unsupported SDMX availability component \"destinationCountry\""),
	}
	svc := newSdmxTestService(ds)

	_, err := svc.Availability(context.Background(), sdmxAvailabilityRequest("destinationCountry", "c[variableMeasured]=Count_Person"))
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Availability() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
	}
	if !strings.Contains(status.Convert(err).Message(), "destinationCountry") {
		t.Fatalf("Availability() message = %q, want component name", status.Convert(err).Message())
	}
}

func TestSdmxBackendInfrastructureErrorsAreInternal(t *testing.T) {
	const backendMessage = "spanner permission details"
	tests := []struct {
		name string
		call func(*Service) error
		ds   *sdmxDataSource
	}{
		{
			name: "data",
			ds:   &sdmxDataSource{err: status.Error(codes.PermissionDenied, backendMessage)},
			call: func(svc *Service) error {
				_, err := svc.Data(context.Background(), sdmxDataRequest("c[variableMeasured]=Count_Person"))
				return err
			},
		},
		{
			name: "availability",
			ds:   &sdmxDataSource{availabilityErr: status.Error(codes.PermissionDenied, backendMessage)},
			call: func(svc *Service) error {
				_, err := svc.Availability(context.Background(), sdmxAvailabilityRequest("observationAbout", "c[variableMeasured]=Count_Person"))
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call(newSdmxTestService(tt.ds))
			if status.Code(err) != codes.Internal {
				t.Fatalf("SDMX request code = %v, want %v; err = %v", status.Code(err), codes.Internal, err)
			}
			message := status.Convert(err).Message()
			if strings.Contains(message, backendMessage) {
				t.Fatalf("SDMX request exposed backend message %q", message)
			}
		})
	}
}

func TestAvailabilityNilDispatcher(t *testing.T) {
	svc := New(nil)

	_, err := svc.Availability(context.Background(), sdmxAvailabilityRequest("observationAbout", "c[variableMeasured]=Count_Person"))
	if status.Code(err) != codes.Internal {
		t.Fatalf("Availability() code = %v, want %v; err = %v", status.Code(err), codes.Internal, err)
	}
	if !strings.Contains(status.Convert(err).Message(), "Internal server error occurred while processing the request.") {
		t.Fatalf("Availability() message = %q, want generic internal error", status.Convert(err).Message())
	}
}

func TestAvailabilitySuccess(t *testing.T) {
	ds := &sdmxDataSource{
		availabilityResult: &sdmxpb.SdmxAvailabilityResult{Values: []string{"country/USA", "geoId/06"}},
	}
	svc := newSdmxTestService(ds)

	response, err := svc.Availability(context.Background(), sdmxAvailabilityRequest(
		"observationAbout",
		"c[variableMeasured]=Count_Person,Count_Household&"+
			"c[observationAbout]=country%2FUSA,geoId%2F06&"+
			"c[unit]=Person,Count",
	))
	if err != nil {
		t.Fatalf("Availability() error = %v", err)
	}
	if response.ContentType != sdmxformat.StructureJSONType {
		t.Fatalf("Availability() content type = %q, want %q", response.ContentType, sdmxformat.StructureJSONType)
	}
	for _, want := range []string{"\"id\":\"observationAbout\"", "\"country/USA\"", "\"geoId/06\""} {
		if !strings.Contains(string(response.Body), want) {
			t.Fatalf("Availability() body missing %q: %s", want, string(response.Body))
		}
	}
	wantQuery := &sdmxpb.SdmxAvailabilityQuery{
		ComponentId: "observationAbout",
		Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("Count_Person", "Count_Household"),
			"observationAbout": sdmxComponentConstraint("country/USA", "geoId/06"),
			"unit":             sdmxComponentConstraint("Person", "Count"),
		},
	}
	if diff := cmp.Diff(wantQuery, ds.gotAvailability, protocmp.Transform()); diff != "" {
		t.Fatalf("dispatcher query mismatch (-want +got):\n%s", diff)
	}
}

func TestAvailabilitySelectsOtherDimension(t *testing.T) {
	ds := &sdmxDataSource{
		availabilityResult: &sdmxpb.SdmxAvailabilityResult{Values: []string{"Person"}},
	}
	svc := newSdmxTestService(ds)

	response, err := svc.Availability(context.Background(), sdmxAvailabilityRequest("unit", "c[variableMeasured]=Count_Person"))
	if err != nil {
		t.Fatalf("Availability() error = %v", err)
	}
	if !strings.Contains(string(response.Body), "\"id\":\"unit\"") {
		t.Fatalf("Availability() body missing unit id: %s", string(response.Body))
	}
	wantQuery := &sdmxpb.SdmxAvailabilityQuery{
		ComponentId: "unit",
		Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("Count_Person"),
		},
	}
	if diff := cmp.Diff(wantQuery, ds.gotAvailability, protocmp.Transform()); diff != "" {
		t.Fatalf("dispatcher query mismatch (-want +got):\n%s", diff)
	}
}

func TestAvailabilitySelectsDynamicDimension(t *testing.T) {
	ds := &sdmxDataSource{
		availabilityResult: &sdmxpb.SdmxAvailabilityResult{Values: []string{"country/CAN"}},
	}
	svc := newSdmxTestService(ds)

	response, err := svc.Availability(context.Background(), sdmxAvailabilityRequest(
		"destinationCountry",
		"c[variableMeasured]=Count_Person_Migrated&c[sourceCountry]=country%2FUSA",
	))
	if err != nil {
		t.Fatalf("Availability() error = %v", err)
	}
	if !strings.Contains(string(response.Body), "\"id\":\"destinationCountry\"") {
		t.Fatalf("Availability() body missing destinationCountry id: %s", string(response.Body))
	}
	wantQuery := &sdmxpb.SdmxAvailabilityQuery{
		ComponentId: "destinationCountry",
		Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("Count_Person_Migrated"),
			"sourceCountry":    sdmxComponentConstraint("country/USA"),
		},
	}
	if diff := cmp.Diff(wantQuery, ds.gotAvailability, protocmp.Transform()); diff != "" {
		t.Fatalf("dispatcher query mismatch (-want +got):\n%s", diff)
	}
}

func TestAvailabilitySDMXDebugLoggingSuccess(t *testing.T) {
	buf, restore := captureSdmxLogs()
	defer restore()

	ds := &sdmxDataSource{
		availabilityResult: &sdmxpb.SdmxAvailabilityResult{Values: []string{"country/USA"}},
	}
	svc := newSdmxTestService(ds)

	_, err := svc.Availability(context.Background(), withLog(sdmxAvailabilityRequest("observationAbout", "c[variableMeasured]=Count_Person")))
	if err != nil {
		t.Fatalf("Availability() error = %v", err)
	}
	logs := buf.String()
	for _, want := range []string{
		"SDMX availability request parsed",
		"SDMX availability dispatcher request",
		"observationAbout",
		"variableMeasured",
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("logs do not contain %q: %s", want, logs)
		}
	}
}

func TestAvailabilitySDMXDebugLoggingParseFailure(t *testing.T) {
	buf, restore := captureSdmxLogs()
	defer restore()

	svc := newSdmxTestService(&sdmxDataSource{})

	_, err := svc.Availability(context.Background(), withLog(sdmxAvailabilityRequest("observationAbout", "c[variableMeasured]=Count_Person&c[TIME_PERIOD]=2020")))
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("Availability() code = %v, want %v; err = %v", status.Code(err), codes.Unimplemented, err)
	}
	logs := buf.String()
	for _, want := range []string{
		"SDMX availability request parse failed",
		"unsupported SDMX component filter",
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("logs do not contain %q: %s", want, logs)
		}
	}
}

func newSdmxTestService(ds *sdmxDataSource) *Service {
	sources := datasources.NewDataSources([]datasource.DataSource{ds}, nil)
	return New(dispatcher.NewDispatcher(nil, sources))
}

func withAccept(request Request, accept string) Request {
	request.Accept = []string{accept}
	return request
}

func withLog(request Request) Request {
	request.LogSDMX = true
	return request
}

func sdmxDataRequest(query string) Request {
	return Request{
		Tail:        sdmxDataTail(),
		OriginalURI: sdmxDataURI(query),
	}
}

func sdmxAvailabilityRequest(componentID string, query string) Request {
	return Request{
		Tail:        sdmxAvailabilityTail(componentID),
		OriginalURI: sdmxAvailabilityURI(componentID, query),
	}
}

func sdmxDataTail() string {
	return "dataflow/DC/DF_OBS/1.0.0/*"
}

func sdmxDataURI(query string) string {
	return "/sdmx/v3/data/" + sdmxDataTail() + "?" + query
}

func sdmxAvailabilityTail(componentID string) string {
	return sdmxDataTail() + "/" + componentID
}

func sdmxAvailabilityURI(componentID string, query string) string {
	return "/sdmx/v3/availability/" + sdmxAvailabilityTail(componentID) + "?" + query
}

func captureSdmxLogs() (*bytes.Buffer, func()) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	originalLogger := slog.Default()
	slog.SetDefault(logger)
	return &buf, func() { slog.SetDefault(originalLogger) }
}
