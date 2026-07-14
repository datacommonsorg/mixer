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
	"context"
	"strings"
	"testing"

	"github.com/datacommonsorg/mixer/internal/featureflags"
	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	sdmxformat "github.com/datacommonsorg/mixer/internal/server/sdmx/format"
	httpbody "google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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
	result              *sdmxpb.SdmxDataResult
	availabilityResult  *sdmxpb.SdmxAvailabilityResult
	dataRequest         *sdmxpb.SdmxDataQuery
	availabilityRequest *sdmxpb.SdmxAvailabilityQuery
}

func (ds *sdmxDataSource) Type() datasource.DataSourceType {
	return datasource.TypeMock
}

func (ds *sdmxDataSource) Id() string {
	return "sdmx-test"
}

func (ds *sdmxDataSource) SdmxData(ctx context.Context, req *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error) {
	ds.dataRequest = req
	return ds.result, nil
}

func (ds *sdmxDataSource) SdmxAvailability(ctx context.Context, req *sdmxpb.SdmxAvailabilityQuery) (*sdmxpb.SdmxAvailabilityResult, error) {
	ds.availabilityRequest = req
	return ds.availabilityResult, nil
}

func TestV3SdmxDataFeatureFlag(t *testing.T) {
	server := &Server{flags: &featureflags.Flags{EnableSDMXDataApi: false}}
	stream := &sdmxDataStream{ctx: context.Background()}

	err := server.V3SdmxData(&sdmxpb.SdmxRestRequest{}, stream)
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("V3SdmxData() code = %v, want %v; err = %v", status.Code(err), codes.Unimplemented, err)
	}
	if !strings.Contains(status.Convert(err).Message(), "SDMX API is not enabled") {
		t.Fatalf("V3SdmxData() message = %q, want disabled message", status.Convert(err).Message())
	}
}

func TestV3SdmxDataNilRequest(t *testing.T) {
	server := newSdmxHandlerTestServer(&sdmxDataSource{})
	stream := &sdmxDataStream{ctx: context.Background()}

	err := server.V3SdmxData(nil, stream)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("V3SdmxData() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
	}
	if !strings.Contains(status.Convert(err).Message(), "request cannot be nil") {
		t.Fatalf("V3SdmxData() message = %q, want nil request message", status.Convert(err).Message())
	}
}

func TestV3SdmxDataWrapsServiceResponse(t *testing.T) {
	server := newSdmxHandlerTestServer(&sdmxDataSource{
		result: testSdmxDataResult([]string{datacommons.ComponentObservationAbout}),
	})
	stream := &sdmxDataStream{
		ctx: sdmxIncomingContext(sdmxDataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA")),
	}

	err := server.V3SdmxData(&sdmxpb.SdmxRestRequest{Tail: sdmxDataTail()}, stream)
	if err != nil {
		t.Fatalf("V3SdmxData() error = %v", err)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent %d HttpBody messages, want 1", len(stream.sent))
	}
	if stream.sent[0].GetContentType() != sdmxformat.CSVContentType {
		t.Fatalf("ContentType = %q, want %q", stream.sent[0].GetContentType(), sdmxformat.CSVContentType)
	}
	if got := string(stream.sent[0].GetData()); !strings.HasPrefix(got, "STRUCTURE,STRUCTURE_ID,ACTION,variableMeasured,observationAbout") {
		t.Fatalf("Data = %q, want SDMX CSV header", got)
	}
}

func testSdmxDataResult(observationProperties []string) *sdmxpb.SdmxDataResult {
	components := datacommons.DataComponentsForObservationProperties(observationProperties)
	result := &sdmxpb.SdmxDataResult{
		Shape: &sdmxpb.SdmxDataShape{
			Components: make([]*sdmxpb.SdmxComponent, 0, len(components)),
		},
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

func TestV3SdmxAvailabilityFeatureFlag(t *testing.T) {
	server := &Server{flags: &featureflags.Flags{EnableSDMXDataApi: false}}

	_, err := server.V3SdmxAvailability(context.Background(), &sdmxpb.SdmxRestRequest{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("V3SdmxAvailability() code = %v, want %v; err = %v", status.Code(err), codes.Unimplemented, err)
	}
	if !strings.Contains(status.Convert(err).Message(), "SDMX API is not enabled") {
		t.Fatalf("V3SdmxAvailability() message = %q, want disabled message", status.Convert(err).Message())
	}
}

func TestV3SdmxAvailabilityNilRequest(t *testing.T) {
	server := newSdmxHandlerTestServer(&sdmxDataSource{})

	_, err := server.V3SdmxAvailability(context.Background(), nil)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("V3SdmxAvailability() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
	}
	if !strings.Contains(status.Convert(err).Message(), "request cannot be nil") {
		t.Fatalf("V3SdmxAvailability() message = %q, want nil request message", status.Convert(err).Message())
	}
}

func TestV3SdmxAvailabilityWrapsServiceResponse(t *testing.T) {
	server := newSdmxHandlerTestServer(&sdmxDataSource{
		availabilityResult: &sdmxpb.SdmxAvailabilityResult{Values: []string{"country/USA"}},
	})

	body, err := server.V3SdmxAvailability(
		sdmxIncomingContext(sdmxAvailabilityURI("observationAbout", "c[variableMeasured]=Count_Person")),
		&sdmxpb.SdmxRestRequest{Tail: sdmxAvailabilityTail("observationAbout")},
	)
	if err != nil {
		t.Fatalf("V3SdmxAvailability() error = %v", err)
	}
	if body.GetContentType() != sdmxformat.StructureJSONType {
		t.Fatalf("ContentType = %q, want %q", body.GetContentType(), sdmxformat.StructureJSONType)
	}
	if !strings.Contains(string(body.GetData()), "\"country/USA\"") {
		t.Fatalf("Data missing value: %s", string(body.GetData()))
	}
}

func TestSdmxDataFeatureFlagPrecedesValidation(t *testing.T) {
	server := &Server{flags: &featureflags.Flags{EnableSDMXDataApi: false}}

	_, err := server.SdmxData(context.Background(), nil)
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("SdmxData() code = %v, want %v; err = %v", status.Code(err), codes.Unimplemented, err)
	}
	if got, want := status.Convert(err).Message(), "SDMX API is not enabled"; got != want {
		t.Fatalf("SdmxData() message = %q, want %q", got, want)
	}
}

func TestSdmxDataForwardsStructuredRequest(t *testing.T) {
	ds := &sdmxDataSource{result: testSdmxDataResult([]string{datacommons.ComponentObservationAbout})}
	server := newSdmxHandlerTestServer(ds)
	request := &sdmxpb.SdmxDataQuery{
		Constraints: map[string]*sdmxpb.ConstraintList{
			datacommons.ComponentVariableMeasured: {Values: []string{"Count_Person"}},
		},
	}

	got, err := server.SdmxData(context.Background(), request)
	if err != nil {
		t.Fatalf("SdmxData() error = %v", err)
	}
	if !proto.Equal(ds.dataRequest, request) {
		t.Fatalf("SdmxData() request = %v, want %v", ds.dataRequest, request)
	}
	if !proto.Equal(got, ds.result) {
		t.Fatalf("SdmxData() response = %v, want %v", got, ds.result)
	}
}

func TestSdmxDataValidation(t *testing.T) {
	server := newSdmxHandlerTestServer(&sdmxDataSource{})

	_, err := server.SdmxData(context.Background(), nil)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("SdmxData(nil) code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
	}

	server.dispatcher = nil
	_, err = server.SdmxData(context.Background(), &sdmxpb.SdmxDataQuery{})
	if status.Code(err) != codes.Internal {
		t.Fatalf("SdmxData() code = %v, want %v; err = %v", status.Code(err), codes.Internal, err)
	}
	if got, want := status.Convert(err).Message(), "Internal server error occurred while processing the request."; got != want {
		t.Fatalf("SdmxData() message = %q, want %q", got, want)
	}
}

func TestSdmxAvailabilityFeatureFlagPrecedesValidation(t *testing.T) {
	server := &Server{flags: &featureflags.Flags{EnableSDMXDataApi: false}}

	_, err := server.SdmxAvailability(context.Background(), nil)
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("SdmxAvailability() code = %v, want %v; err = %v", status.Code(err), codes.Unimplemented, err)
	}
	if got, want := status.Convert(err).Message(), "SDMX API is not enabled"; got != want {
		t.Fatalf("SdmxAvailability() message = %q, want %q", got, want)
	}
}

func TestSdmxAvailabilityForwardsStructuredRequest(t *testing.T) {
	ds := &sdmxDataSource{
		availabilityResult: &sdmxpb.SdmxAvailabilityResult{Values: []string{"country/USA"}},
	}
	server := newSdmxHandlerTestServer(ds)
	request := &sdmxpb.SdmxAvailabilityQuery{
		ComponentId: datacommons.ComponentObservationAbout,
		Constraints: map[string]*sdmxpb.ConstraintList{
			datacommons.ComponentVariableMeasured: {Values: []string{"Count_Person"}},
		},
	}

	got, err := server.SdmxAvailability(context.Background(), request)
	if err != nil {
		t.Fatalf("SdmxAvailability() error = %v", err)
	}
	if !proto.Equal(ds.availabilityRequest, request) {
		t.Fatalf("SdmxAvailability() request = %v, want %v", ds.availabilityRequest, request)
	}
	if !proto.Equal(got, ds.availabilityResult) {
		t.Fatalf("SdmxAvailability() response = %v, want %v", got, ds.availabilityResult)
	}
}

func TestSdmxAvailabilityValidation(t *testing.T) {
	server := newSdmxHandlerTestServer(&sdmxDataSource{})

	_, err := server.SdmxAvailability(context.Background(), nil)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("SdmxAvailability(nil) code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
	}

	server.dispatcher = nil
	_, err = server.SdmxAvailability(context.Background(), &sdmxpb.SdmxAvailabilityQuery{})
	if status.Code(err) != codes.Internal {
		t.Fatalf("SdmxAvailability() code = %v, want %v; err = %v", status.Code(err), codes.Internal, err)
	}
	if got, want := status.Convert(err).Message(), "Internal server error occurred while processing the request."; got != want {
		t.Fatalf("SdmxAvailability() message = %q, want %q", got, want)
	}
}

func newSdmxHandlerTestServer(ds *sdmxDataSource) *Server {
	sources := datasources.NewDataSources([]datasource.DataSource{ds}, nil)
	return &Server{
		flags:      &featureflags.Flags{EnableSDMXDataApi: true},
		dispatcher: dispatcher.NewDispatcher(nil, sources),
	}
}

func sdmxIncomingContext(originalURI string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-envoy-original-path", originalURI))
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
