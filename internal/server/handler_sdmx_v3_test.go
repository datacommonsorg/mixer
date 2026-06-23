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
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv3 "github.com/datacommonsorg/mixer/internal/proto/v3"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
	sdmxformat "github.com/datacommonsorg/mixer/internal/server/sdmx/format"
	httpbody "google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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
	result             *pb.SdmxDataResult
	availabilityResult *pb.SdmxAvailabilityResult
}

func (ds *sdmxDataSource) Type() datasource.DataSourceType {
	return datasource.TypeMock
}

func (ds *sdmxDataSource) Id() string {
	return "sdmx-test"
}

func (ds *sdmxDataSource) SdmxData(ctx context.Context, req *pb.SdmxDataQuery) (*pb.SdmxDataResult, error) {
	return ds.result, nil
}

func (ds *sdmxDataSource) SdmxAvailability(ctx context.Context, req *pb.SdmxAvailabilityQuery) (*pb.SdmxAvailabilityResult, error) {
	return ds.availabilityResult, nil
}

func TestV3SdmxDataFeatureFlag(t *testing.T) {
	server := &Server{flags: &featureflags.Flags{EnableSDMXDataApi: false}}
	stream := &sdmxDataStream{ctx: context.Background()}

	err := server.V3SdmxData(&pbv3.SdmxRestRequest{}, stream)
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("V3SdmxData() code = %v, want %v; err = %v", status.Code(err), codes.Unimplemented, err)
	}
	if !strings.Contains(status.Convert(err).Message(), "SDMX API is not enabled") {
		t.Fatalf("V3SdmxData() message = %q, want disabled message", status.Convert(err).Message())
	}
}

func TestV3SdmxDataWrapsServiceResponse(t *testing.T) {
	server := newSdmxHandlerTestServer(&sdmxDataSource{result: &pb.SdmxDataResult{}})
	stream := &sdmxDataStream{
		ctx: sdmxIncomingContext(sdmxDataURI("c[variableMeasured]=Count_Person&c[observationAbout]=country%2FUSA")),
	}

	err := server.V3SdmxData(&pbv3.SdmxRestRequest{Tail: sdmxDataTail()}, stream)
	if err != nil {
		t.Fatalf("V3SdmxData() error = %v", err)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent %d HttpBody messages, want 1", len(stream.sent))
	}
	if stream.sent[0].GetContentType() != sdmxformat.JSONStatContentType {
		t.Fatalf("ContentType = %q, want %q", stream.sent[0].GetContentType(), sdmxformat.JSONStatContentType)
	}
	if got := string(stream.sent[0].GetData()); got != "{}" {
		t.Fatalf("Data = %q, want {}", got)
	}
}

func TestV3SdmxAvailabilityFeatureFlag(t *testing.T) {
	server := &Server{flags: &featureflags.Flags{EnableSDMXDataApi: false}}

	_, err := server.V3SdmxAvailability(context.Background(), &pbv3.SdmxRestRequest{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("V3SdmxAvailability() code = %v, want %v; err = %v", status.Code(err), codes.Unimplemented, err)
	}
	if !strings.Contains(status.Convert(err).Message(), "SDMX API is not enabled") {
		t.Fatalf("V3SdmxAvailability() message = %q, want disabled message", status.Convert(err).Message())
	}
}

func TestV3SdmxAvailabilityWrapsServiceResponse(t *testing.T) {
	server := newSdmxHandlerTestServer(&sdmxDataSource{
		availabilityResult: &pb.SdmxAvailabilityResult{Values: []string{"country/USA"}},
	})

	body, err := server.V3SdmxAvailability(
		sdmxIncomingContext(sdmxAvailabilityURI("observationAbout", "c[variableMeasured]=Count_Person")),
		&pbv3.SdmxRestRequest{Tail: sdmxAvailabilityTail("observationAbout")},
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
