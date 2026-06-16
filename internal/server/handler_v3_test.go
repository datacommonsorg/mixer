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
	"errors"
	"strings"
	"testing"

	"github.com/datacommonsorg/mixer/internal/featureflags"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv3 "github.com/datacommonsorg/mixer/internal/proto/v3"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
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
			name:       "Missing supported constraints",
			ctx:        sdmxIncomingContext("/sdmx/v3/data?dimensionAtObservation=AllDimensions"),
			request:    &pbv3.SdmxRestRequest{},
			enabled:    true,
			wantCode:   codes.InvalidArgument,
			wantErrSub: "At least one constraint or variableMeasured is required.",
		},
		{
			name:       "Unsupported AND filter",
			ctx:        sdmxIncomingContext("/sdmx/v3/data?c[FREQ]=A+M"),
			request:    &pbv3.SdmxRestRequest{},
			enabled:    true,
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX AND filters are not implemented yet",
		},
		{
			name:       "Unsupported operator",
			ctx:        sdmxIncomingContext("/sdmx/v3/data?c[TIME_PERIOD]=ge:2020"),
			request:    &pbv3.SdmxRestRequest{},
			enabled:    true,
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX component filter operators are not implemented yet",
		},
		{
			name:       "Unsupported observation value filter",
			ctx:        sdmxIncomingContext("/sdmx/v3/data?c[OBS_VALUE]=10"),
			request:    &pbv3.SdmxRestRequest{},
			enabled:    true,
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX observation value filters are not implemented yet",
		},
		{
			name:       "Unsupported non star key",
			ctx:        sdmxIncomingContext("/sdmx/v3/data/dataflow/DATACOMMONS/DF_OBSERVATIONS/1.0/A.US?c[FREQ]=A"),
			request:    &pbv3.SdmxRestRequest{Tail: "dataflow/DATACOMMONS/DF_OBSERVATIONS/1.0/A.US"},
			enabled:    true,
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX data keys other than * are not implemented yet",
		},
		{
			name:       "Unsupported SDMX media type",
			ctx:        sdmxIncomingContextWithAccept("/sdmx/v3/data?c[FREQ]=A", "application/vnd.sdmx.data+json;version=2.0.0"),
			request:    &pbv3.SdmxRestRequest{},
			enabled:    true,
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX JSON and CSV responses are not implemented yet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &Server{flags: &featureflags.Flags{EnableSDMXDataApi: tt.enabled}}
			stream := &sdmxDataStream{ctx: tt.ctx}
			err := server.V3SdmxData(tt.request, stream)
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
		ctx: sdmxIncomingContext("/sdmx/v3/data/dataflow/DATACOMMONS/DF_OBSERVATIONS/1.0/*?c[FREQ]=A,M&c[geo]=country%2FUSA&c[TIME_PERIOD]=2020"),
	}

	err := server.V3SdmxData(
		&pbv3.SdmxRestRequest{Tail: "dataflow/DATACOMMONS/DF_OBSERVATIONS/1.0/*"},
		stream,
	)
	if err != nil {
		t.Fatalf("V3SdmxData() error = %v", err)
	}

	wantQuery := &pb.SdmxDataQuery{
		Constraints: map[string]*pb.ConstraintList{
			"FREQ":            &pb.ConstraintList{Values: []string{"A", "M"}},
			"geo":             &pb.ConstraintList{Values: []string{"country/USA"}},
			"observationDate": &pb.ConstraintList{Values: []string{"2020"}},
		},
	}
	if diff := cmp.Diff(wantQuery, ds.got, protocmp.Transform()); diff != "" {
		t.Errorf("SdmxData query mismatch (-want +got):\n%s", diff)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent %d HttpBody messages, want 1", len(stream.sent))
	}
	if stream.sent[0].GetContentType() != sdmxJSONStatContentType {
		t.Errorf("ContentType = %q, want %q", stream.sent[0].GetContentType(), sdmxJSONStatContentType)
	}
	if !strings.Contains(string(stream.sent[0].GetData()), "\"version\":\"2.0\"") {
		t.Errorf("response does not look like JSON-stat: %s", stream.sent[0].GetData())
	}
}

func TestV3SdmxData_EmptyResult(t *testing.T) {
	ds := &sdmxDataSource{result: &pb.SdmxDataResult{}}
	server := newSdmxTestServer(ds)
	stream := &sdmxDataStream{ctx: sdmxIncomingContext("/sdmx/v3/data?c[FREQ]=A")}

	err := server.V3SdmxData(&pbv3.SdmxRestRequest{}, stream)
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

func TestV3SdmxData_DispatcherError(t *testing.T) {
	ds := &sdmxDataSource{err: errors.New("dispatcher failed")}
	server := newSdmxTestServer(ds)
	stream := &sdmxDataStream{ctx: sdmxIncomingContext("/sdmx/v3/data?c[FREQ]=A")}

	err := server.V3SdmxData(&pbv3.SdmxRestRequest{}, stream)
	if status.Code(err) != codes.Internal {
		t.Fatalf("V3SdmxData() code = %v, want %v; err = %v", status.Code(err), codes.Internal, err)
	}
}

func newSdmxTestServer(ds *sdmxDataSource) *Server {
	sources := datasources.NewDataSources([]datasource.DataSource{ds}, nil)
	return &Server{
		flags:      &featureflags.Flags{EnableSDMXDataApi: true},
		dispatcher: dispatcher.NewDispatcher(nil, sources),
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
