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

package server

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/datacommonsorg/mixer/internal/featureflags"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv3 "github.com/datacommonsorg/mixer/internal/proto/v3"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
	"github.com/datacommonsorg/mixer/internal/server/sdmx"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/google/go-cmp/cmp"
	httpbody "google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

type sdmxDataStream struct {
	ctx  context.Context
	sent []*httpbody.HttpBody
}

func (s *sdmxDataStream) Context() context.Context {
	return s.ctx
}

func (s *sdmxDataStream) Send(body *httpbody.HttpBody) error {
	s.sent = append(s.sent, body)
	return nil
}

func (s *sdmxDataStream) SetHeader(metadata.MD) error {
	return nil
}

func (s *sdmxDataStream) SendHeader(metadata.MD) error {
	return nil
}

func (s *sdmxDataStream) SetTrailer(metadata.MD) {
}

func (s *sdmxDataStream) SendMsg(any) error {
	return nil
}

func (s *sdmxDataStream) RecvMsg(any) error {
	return nil
}

type sdmxDataSource struct {
	datasource.DataSource
	result *pb.SdmxDataResult
	err    error
	got    *pb.SdmxDataQuery
}

func (ds *sdmxDataSource) Type() datasource.DataSourceType {
	return datasource.TypeMock
}

func (ds *sdmxDataSource) Id() string {
	return "sdmx-test"
}

func (ds *sdmxDataSource) SdmxData(ctx context.Context, req *pb.SdmxDataQuery) (*pb.SdmxDataResult, error) {
	ds.got = req
	return ds.result, ds.err
}

func TestV3SdmxData_Validation(t *testing.T) {
	tests := []struct {
		name       string
		ctx        context.Context
		request    *pbv3.SdmxRestRequest
		enabled    bool
		wantCode   codes.Code
		wantErrSub string
	}{
		{
			name:       "API not enabled",
			ctx:        context.Background(),
			request:    &pbv3.SdmxRestRequest{},
			enabled:    false,
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX API is not enabled",
		},
		{
			name:       "Missing original URI",
			ctx:        context.Background(),
			request:    &pbv3.SdmxRestRequest{},
			enabled:    true,
			wantCode:   codes.InvalidArgument,
			wantErrSub: "missing SDMX request URI",
		},
		{
			name:       "Missing variable measured",
			ctx:        sdmxIncomingContext(sdmxDataURI("c[observationAbout]=country%2FUSA")),
			request:    &pbv3.SdmxRestRequest{},
			enabled:    true,
			wantCode:   codes.InvalidArgument,
			wantErrSub: "missing required SDMX component filter variableMeasured",
		},
		{
			name:       "Unsupported AND filter",
			ctx:        sdmxIncomingContext(sdmxDataURI("c[variableMeasured]=Count+Person&c[observationAbout]=country%2FUSA")),
			request:    &pbv3.SdmxRestRequest{},
			enabled:    true,
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX AND filters are not implemented yet",
		},
		{
			name:       "Unsupported operator",
			ctx:        sdmxIncomingContext(sdmxDataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA&c[TIME_PERIOD]=ge:2020")),
			request:    &pbv3.SdmxRestRequest{},
			enabled:    true,
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX component filter operators are not implemented yet",
		},
		{
			name:       "Unsupported observation value filter",
			ctx:        sdmxIncomingContext(sdmxDataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA&c[OBS_VALUE]=10")),
			request:    &pbv3.SdmxRestRequest{},
			enabled:    true,
			wantCode:   codes.Unimplemented,
			wantErrSub: "unsupported SDMX component filter",
		},
		{
			name:       "Unsupported geo filter",
			ctx:        sdmxIncomingContext(sdmxDataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA&c[geo]=country%2FUSA")),
			request:    &pbv3.SdmxRestRequest{},
			enabled:    true,
			wantCode:   codes.Unimplemented,
			wantErrSub: "unsupported SDMX component filter",
		},
		{
			name:       "Unsupported non star key",
			ctx:        sdmxIncomingContext("/sdmx/v3/data/dataflow/DATACOMMONS/DF_OBSERVATIONS/1.0.0/A.US?c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"),
			request:    &pbv3.SdmxRestRequest{Tail: "dataflow/DATACOMMONS/DF_OBSERVATIONS/1.0.0/A.US"},
			enabled:    true,
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX data keys other than * are not implemented yet",
		},
		{
			name:       "Unsupported SDMX JSON media type",
			ctx:        sdmxIncomingContextWithAccept(sdmxDataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"), "application/vnd.sdmx.data+json;version=2.0.0"),
			request:    &pbv3.SdmxRestRequest{},
			enabled:    true,
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX JSON responses are not implemented yet",
		},
		{
			name:       "Unsupported SDMX CSV option",
			ctx:        sdmxIncomingContextWithAccept(sdmxDataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"), "application/vnd.sdmx.data+csv;version=2.0.0;labels=name"),
			request:    &pbv3.SdmxRestRequest{},
			enabled:    true,
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX CSV response option",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &Server{flags: &featureflags.Flags{EnableSDMXDataApi: tt.enabled}}
			stream := &sdmxDataStream{ctx: tt.ctx}
			request := tt.request
			if request.GetTail() == "" {
				request = &pbv3.SdmxRestRequest{Tail: sdmxDataTail()}
			}
			err := server.V3SdmxData(request, stream)
			if err == nil {
				t.Fatal("V3SdmxData() error = nil, want error")
			}

			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("V3SdmxData() error is not a status error: %v", err)
			}
			if st.Code() != tt.wantCode {
				t.Errorf("V3SdmxData() code = %v, want %v", st.Code(), tt.wantCode)
			}
			if !strings.Contains(st.Message(), tt.wantErrSub) {
				t.Errorf("V3SdmxData() message = %q, want substring %q", st.Message(), tt.wantErrSub)
			}
		})
	}
}

func TestV3SdmxData_Success(t *testing.T) {
	ds := &sdmxDataSource{
		result: &pb.SdmxDataResult{
			Observations: []*pb.SdmxObservation{
				{
					VariableMeasured: "Count_Person",
					Provenance:       "dc/base",
					DatesAndValues: []*pb.SdmxDateValue{
						{Date: "2020", Value: "1"},
					},
					Dimensions: map[string]string{"geo": "country/USA"},
				},
			},
		},
	}
	server := newSdmxTestServer(ds)
	stream := &sdmxDataStream{
		ctx: sdmxIncomingContext(sdmxDataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA&c[TIME_PERIOD]=2020,2021")),
	}

	err := server.V3SdmxData(
		&pbv3.SdmxRestRequest{Tail: sdmxDataTail()},
		stream,
	)
	if err != nil {
		t.Fatalf("V3SdmxData() error = %v", err)
	}

	wantQuery := &pb.SdmxDataQuery{
		Constraints: map[string]*pb.ConstraintList{
			"variableMeasured": &pb.ConstraintList{Values: []string{"Count_Person"}},
			"observationAbout": &pb.ConstraintList{Values: []string{"country/USA"}},
			"observationDate":  &pb.ConstraintList{Values: []string{"2020", "2021"}},
		},
	}
	if diff := cmp.Diff(wantQuery, ds.got, protocmp.Transform()); diff != "" {
		t.Errorf("SdmxData query mismatch (-want +got):\n%s", diff)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent %d HttpBody messages, want 1", len(stream.sent))
	}
	if stream.sent[0].GetContentType() != sdmx.JSONStatContentType {
		t.Errorf("ContentType = %q, want %q", stream.sent[0].GetContentType(), sdmx.JSONStatContentType)
	}
	if !strings.Contains(string(stream.sent[0].GetData()), "\"version\":\"2.0\"") {
		t.Errorf("response does not look like JSON-stat: %s", stream.sent[0].GetData())
	}
}

func TestV3SdmxData_CSVSuccess(t *testing.T) {
	ds := &sdmxDataSource{
		result: &pb.SdmxDataResult{
			Observations: []*pb.SdmxObservation{
				{
					VariableMeasured: "Count_Person",
					Provenance:       "dc/base",
					DatesAndValues: []*pb.SdmxDateValue{
						{Date: "2020", Value: "1.50"},
					},
					Dimensions: map[string]string{
						"observationAbout": "country/USA",
					},
					Attributes: map[string]string{
						"unit":              "Person",
						"measurementMethod": "Census",
						"observationPeriod": "P1Y",
						"scalingFactor":     "0",
					},
				},
			},
		},
	}
	server := newSdmxTestServer(ds)
	stream := &sdmxDataStream{
		ctx: sdmxIncomingContextWithAccept(
			sdmxDataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"),
			"application/vnd.sdmx.data+csv;version=2.0.0",
		),
	}

	err := server.V3SdmxData(&pbv3.SdmxRestRequest{Tail: sdmxDataTail()}, stream)
	if err != nil {
		t.Fatalf("V3SdmxData() error = %v", err)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent %d HttpBody messages, want 1", len(stream.sent))
	}
	if stream.sent[0].GetContentType() != sdmx.CSVContentType {
		t.Errorf("ContentType = %q, want %q", stream.sent[0].GetContentType(), sdmx.CSVContentType)
	}

	want := "STRUCTURE,STRUCTURE_ID,ACTION,variableMeasured,observationAbout,unit,measurementMethod,observationPeriod,provenance,TIME_PERIOD,OBS_VALUE,scalingFactor\r\n" +
		"dataflow,DATACOMMONS:DF_OBSERVATIONS(1.0.0),I,Count_Person,country/USA,Person,Census,P1Y,dc/base,2020,1.50,0\r\n"
	if got := string(stream.sent[0].GetData()); got != want {
		t.Errorf("HttpBody data = %q, want %q", got, want)
	}
}

func TestV3SdmxData_CSVFormatQuery(t *testing.T) {
	ds := &sdmxDataSource{result: &pb.SdmxDataResult{}}
	server := newSdmxTestServer(ds)
	stream := &sdmxDataStream{
		ctx: sdmxIncomingContext(sdmxDataURI("format=csv&c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA")),
	}

	err := server.V3SdmxData(&pbv3.SdmxRestRequest{Tail: sdmxDataTail()}, stream)
	if err != nil {
		t.Fatalf("V3SdmxData() error = %v", err)
	}
	if stream.sent[0].GetContentType() != sdmx.CSVContentType {
		t.Errorf("ContentType = %q, want %q", stream.sent[0].GetContentType(), sdmx.CSVContentType)
	}
	if got := string(stream.sent[0].GetData()); got != "STRUCTURE,STRUCTURE_ID,ACTION,variableMeasured,observationAbout,unit,measurementMethod,observationPeriod,provenance,TIME_PERIOD,OBS_VALUE,scalingFactor\r\n" {
		t.Errorf("HttpBody data = %q, want header-only CSV", got)
	}
}

func TestV3SdmxData_JSONStatFormatQuery(t *testing.T) {
	ds := &sdmxDataSource{result: &pb.SdmxDataResult{}}
	server := newSdmxTestServer(ds)
	stream := &sdmxDataStream{
		ctx: sdmxIncomingContext(sdmxDataURI("format=json-stat&c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA")),
	}

	err := server.V3SdmxData(&pbv3.SdmxRestRequest{Tail: sdmxDataTail()}, stream)
	if err != nil {
		t.Fatalf("V3SdmxData() error = %v", err)
	}
	if stream.sent[0].GetContentType() != sdmx.JSONStatContentType {
		t.Errorf("ContentType = %q, want %q", stream.sent[0].GetContentType(), sdmx.JSONStatContentType)
	}
	if got := string(stream.sent[0].GetData()); got != "{}" {
		t.Errorf("HttpBody data = %q, want empty JSON object", got)
	}
}

func TestV3SdmxData_FormatQueryOverridesAccept(t *testing.T) {
	ds := &sdmxDataSource{result: &pb.SdmxDataResult{}}
	server := newSdmxTestServer(ds)
	stream := &sdmxDataStream{
		ctx: sdmxIncomingContextWithAccept(
			sdmxDataURI("format=csv&c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"),
			"application/vnd.sdmx.data+json;version=2.0.0",
		),
	}

	err := server.V3SdmxData(&pbv3.SdmxRestRequest{Tail: sdmxDataTail()}, stream)
	if err != nil {
		t.Fatalf("V3SdmxData() error = %v", err)
	}
	if stream.sent[0].GetContentType() != sdmx.CSVContentType {
		t.Errorf("ContentType = %q, want %q", stream.sent[0].GetContentType(), sdmx.CSVContentType)
	}
}

func TestV3SdmxData_EmptyResult(t *testing.T) {
	ds := &sdmxDataSource{result: &pb.SdmxDataResult{}}
	server := newSdmxTestServer(ds)
	stream := &sdmxDataStream{ctx: sdmxIncomingContext(sdmxDataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"))}

	err := server.V3SdmxData(&pbv3.SdmxRestRequest{Tail: sdmxDataTail()}, stream)
	if err != nil {
		t.Fatalf("V3SdmxData() error = %v", err)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent %d HttpBody messages, want 1", len(stream.sent))
	}
	if got := string(stream.sent[0].GetData()); got != "{}" {
		t.Errorf("HttpBody data = %q, want {}", got)
	}
}

func TestV3SdmxData_CSVEmptyResult(t *testing.T) {
	ds := &sdmxDataSource{result: &pb.SdmxDataResult{}}
	server := newSdmxTestServer(ds)
	stream := &sdmxDataStream{
		ctx: sdmxIncomingContextWithAccept(
			sdmxDataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"),
			"application/vnd.sdmx.data+csv;version=2.0.0",
		),
	}

	err := server.V3SdmxData(&pbv3.SdmxRestRequest{Tail: sdmxDataTail()}, stream)
	if err != nil {
		t.Fatalf("V3SdmxData() error = %v", err)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent %d HttpBody messages, want 1", len(stream.sent))
	}
	if got := string(stream.sent[0].GetData()); got != "STRUCTURE,STRUCTURE_ID,ACTION,variableMeasured,observationAbout,unit,measurementMethod,observationPeriod,provenance,TIME_PERIOD,OBS_VALUE,scalingFactor\r\n" {
		t.Errorf("HttpBody data = %q, want header-only CSV", got)
	}
}

func TestV3SdmxData_DispatcherError(t *testing.T) {
	ds := &sdmxDataSource{err: errors.New("dispatcher failed")}
	server := newSdmxTestServer(ds)
	stream := &sdmxDataStream{ctx: sdmxIncomingContext(sdmxDataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"))}

	err := server.V3SdmxData(&pbv3.SdmxRestRequest{Tail: sdmxDataTail()}, stream)
	if status.Code(err) != codes.Internal {
		t.Fatalf("V3SdmxData() code = %v, want %v; err = %v", status.Code(err), codes.Internal, err)
	}
}

func TestV3SdmxData_SDMXDebugLoggingDisabled(t *testing.T) {
	buf, restore := captureSdmxLogs()
	defer restore()

	ds := &sdmxDataSource{result: &pb.SdmxDataResult{}}
	server := newSdmxTestServer(ds)
	stream := &sdmxDataStream{ctx: sdmxIncomingContext(sdmxDataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA"))}

	err := server.V3SdmxData(&pbv3.SdmxRestRequest{Tail: sdmxDataTail()}, stream)
	if err != nil {
		t.Fatalf("V3SdmxData() error = %v", err)
	}
	logs := buf.String()
	if strings.Contains(logs, "SDMX data request parsed") || strings.Contains(logs, "SDMX data dispatcher request") {
		t.Fatalf("unexpected SDMX debug logs without %s header: %s", util.XLogSDMX, logs)
	}
}

func TestV3SdmxData_SDMXDebugLoggingSuccess(t *testing.T) {
	buf, restore := captureSdmxLogs()
	defer restore()

	ds := &sdmxDataSource{result: &pb.SdmxDataResult{}}
	server := newSdmxTestServer(ds)
	stream := &sdmxDataStream{
		ctx: sdmxIncomingContextWithSDMXLog(sdmxDataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA&c[TIME_PERIOD]=2020")),
	}

	err := server.V3SdmxData(&pbv3.SdmxRestRequest{Tail: sdmxDataTail()}, stream)
	if err != nil {
		t.Fatalf("V3SdmxData() error = %v", err)
	}
	logs := buf.String()
	for _, want := range []string{
		"SDMX data request parsed",
		"original_uri",
		"Count_Person",
		"country/USA",
		"SDMX data dispatcher request",
		"observationDate",
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("logs do not contain %q: %s", want, logs)
		}
	}
}

func TestV3SdmxData_SDMXDebugLoggingParseFailure(t *testing.T) {
	buf, restore := captureSdmxLogs()
	defer restore()

	server := &Server{flags: &featureflags.Flags{EnableSDMXDataApi: true}}
	stream := &sdmxDataStream{
		ctx: sdmxIncomingContextWithSDMXLog(sdmxDataURI("c[observationAbout]=country%2FUSA")),
	}

	err := server.V3SdmxData(&pbv3.SdmxRestRequest{Tail: sdmxDataTail()}, stream)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("V3SdmxData() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
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

func TestV3SdmxData_SDMXDebugLoggingMissingURI(t *testing.T) {
	buf, restore := captureSdmxLogs()
	defer restore()

	server := &Server{flags: &featureflags.Flags{EnableSDMXDataApi: true}}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(util.XLogSDMX, "true"))
	stream := &sdmxDataStream{ctx: ctx}

	err := server.V3SdmxData(&pbv3.SdmxRestRequest{Tail: sdmxDataTail()}, stream)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("V3SdmxData() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
	}
	logs := buf.String()
	for _, want := range []string{
		"SDMX data request URI failed",
		"missing SDMX request URI",
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("logs do not contain %q: %s", want, logs)
		}
	}
}

func TestV3SdmxAvailability_Validation(t *testing.T) {
	tests := []struct {
		name       string
		ctx        context.Context
		request    *pbv3.SdmxRestRequest
		enabled    bool
		wantCode   codes.Code
		wantErrSub string
	}{
		{
			name:       "API not enabled",
			ctx:        context.Background(),
			request:    &pbv3.SdmxRestRequest{},
			enabled:    false,
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX API is not enabled",
		},
		{
			name:       "Missing original URI",
			ctx:        context.Background(),
			request:    &pbv3.SdmxRestRequest{},
			enabled:    true,
			wantCode:   codes.InvalidArgument,
			wantErrSub: "missing SDMX request URI",
		},
		{
			name:       "Missing variable measured",
			ctx:        sdmxIncomingContext(sdmxAvailabilityURI("observationAbout", "c[observationAbout]=country%2FUSA")),
			request:    &pbv3.SdmxRestRequest{},
			enabled:    true,
			wantCode:   codes.InvalidArgument,
			wantErrSub: "missing required SDMX component filter variableMeasured",
		},
		{
			name:       "Unsupported selected component",
			ctx:        sdmxIncomingContext(sdmxAvailabilityURI("OBS_VALUE", "c[variableMeasured]=Count_Person")),
			request:    &pbv3.SdmxRestRequest{Tail: sdmxAvailabilityTail("OBS_VALUE")},
			enabled:    true,
			wantCode:   codes.Unimplemented,
			wantErrSub: "unsupported SDMX availability component",
		},
		{
			name:       "Unsupported mode",
			ctx:        sdmxIncomingContext(sdmxAvailabilityURI("observationAbout", "mode=available&c[variableMeasured]=Count_Person")),
			request:    &pbv3.SdmxRestRequest{},
			enabled:    true,
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX availability mode",
		},
		{
			name:       "Unsupported CSV accept",
			ctx:        sdmxIncomingContextWithAccept(sdmxAvailabilityURI("observationAbout", "c[variableMeasured]=Count_Person"), "application/vnd.sdmx.data+csv;version=2.0.0"),
			request:    &pbv3.SdmxRestRequest{},
			enabled:    true,
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX availability response media type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &Server{
				flags:      &featureflags.Flags{EnableSDMXDataApi: tt.enabled},
				dispatcher: dispatcher.NewDispatcher(nil, nil),
			}
			request := tt.request
			if request.GetTail() == "" {
				request = &pbv3.SdmxRestRequest{Tail: sdmxAvailabilityTail("observationAbout")}
			}
			_, err := server.V3SdmxAvailability(tt.ctx, request)
			if err == nil {
				t.Fatal("V3SdmxAvailability() error = nil, want error")
			}

			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("V3SdmxAvailability() error is not a status error: %v", err)
			}
			if st.Code() != tt.wantCode {
				t.Errorf("V3SdmxAvailability() code = %v, want %v", st.Code(), tt.wantCode)
			}
			if !strings.Contains(st.Message(), tt.wantErrSub) {
				t.Errorf("V3SdmxAvailability() message = %q, want substring %q", st.Message(), tt.wantErrSub)
			}
		})
	}
}

func TestV3SdmxAvailability_BackendUnimplemented(t *testing.T) {
	server := newSdmxAvailabilityTestServer()
	body, err := server.V3SdmxAvailability(
		sdmxIncomingContext(sdmxAvailabilityURI("observationAbout", "c[variableMeasured]=Count_Person")),
		&pbv3.SdmxRestRequest{Tail: sdmxAvailabilityTail("observationAbout")},
	)
	if body != nil {
		t.Fatalf("V3SdmxAvailability() body = %v, want nil", body)
	}
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("V3SdmxAvailability() code = %v, want %v; err = %v", status.Code(err), codes.Unimplemented, err)
	}
	if !strings.Contains(status.Convert(err).Message(), "SDMX availability backend is not implemented yet") {
		t.Fatalf("V3SdmxAvailability() message = %q, want backend unimplemented", status.Convert(err).Message())
	}
}

func TestV3SdmxAvailability_SDMXDebugLoggingSuccess(t *testing.T) {
	buf, restore := captureSdmxLogs()
	defer restore()

	server := newSdmxAvailabilityTestServer()
	_, err := server.V3SdmxAvailability(
		sdmxIncomingContextWithSDMXLog(sdmxAvailabilityURI("TIME_PERIOD", "c[variableMeasured]=Count_Person")),
		&pbv3.SdmxRestRequest{Tail: sdmxAvailabilityTail("TIME_PERIOD")},
	)
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("V3SdmxAvailability() code = %v, want %v; err = %v", status.Code(err), codes.Unimplemented, err)
	}
	logs := buf.String()
	for _, want := range []string{
		"SDMX availability request parsed",
		"SDMX availability dispatcher request",
		"TIME_PERIOD",
		"observationDate",
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("logs do not contain %q: %s", want, logs)
		}
	}
}

func TestV3SdmxAvailability_SDMXDebugLoggingParseFailure(t *testing.T) {
	buf, restore := captureSdmxLogs()
	defer restore()

	server := newSdmxAvailabilityTestServer()
	_, err := server.V3SdmxAvailability(
		sdmxIncomingContextWithSDMXLog(sdmxAvailabilityURI("observationAbout", "c[observationAbout]=country%2FUSA")),
		&pbv3.SdmxRestRequest{Tail: sdmxAvailabilityTail("observationAbout")},
	)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("V3SdmxAvailability() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
	}
	logs := buf.String()
	for _, want := range []string{
		"SDMX availability request parse failed",
		"missing required SDMX component filter variableMeasured",
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("logs do not contain %q: %s", want, logs)
		}
	}
}

func newSdmxTestServer(ds *sdmxDataSource) *Server {
	sources := datasources.NewDataSources([]datasource.DataSource{ds}, nil)
	return &Server{
		flags:      &featureflags.Flags{EnableSDMXDataApi: true},
		dispatcher: dispatcher.NewDispatcher(nil, sources),
	}
}

func newSdmxAvailabilityTestServer() *Server {
	return &Server{
		flags:      &featureflags.Flags{EnableSDMXDataApi: true},
		dispatcher: dispatcher.NewDispatcher(nil, nil),
	}
}

func sdmxIncomingContext(originalURI string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-envoy-original-path", originalURI))
}

func sdmxIncomingContextWithAccept(originalURI string, accept string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"x-envoy-original-path", originalURI,
		"accept", accept,
	))
}

func sdmxIncomingContextWithSDMXLog(originalURI string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"x-envoy-original-path", originalURI,
		util.XLogSDMX, "true",
	))
}

func sdmxDataTail() string {
	return "dataflow/DATACOMMONS/DF_OBSERVATIONS/1.0.0/*"
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
