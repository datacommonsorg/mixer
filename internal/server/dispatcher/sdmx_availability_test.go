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

package dispatcher

import (
	"context"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

type sdmxAvailabilitySource struct {
	datasource.DataSource
	got    *pb.SdmxAvailabilityQuery
	result *pb.SdmxAvailabilityResult
}

func (s *sdmxAvailabilitySource) Type() datasource.DataSourceType {
	return datasource.TypeMock
}

func (s *sdmxAvailabilitySource) Id() string {
	return "availability-test"
}

func (s *sdmxAvailabilitySource) SdmxAvailability(ctx context.Context, req *pb.SdmxAvailabilityQuery) (*pb.SdmxAvailabilityResult, error) {
	s.got = req
	return s.result, nil
}

func TestSdmxAvailabilityBackendUnimplementedWithoutSources(t *testing.T) {
	dispatcher := NewDispatcher(nil, nil)
	got, err := dispatcher.SdmxAvailability(
		context.Background(),
		&pb.SdmxAvailabilityQuery{
			ComponentId: "observationAbout",
			Constraints: map[string]*pb.ConstraintList{
				"variableMeasured": &pb.ConstraintList{Values: []string{"Count_Person"}},
			},
		},
	)

	if got != nil {
		t.Fatalf("SdmxAvailability() response = %v, want nil", got)
	}
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("SdmxAvailability() code = %v, want %v; err = %v", status.Code(err), codes.Unimplemented, err)
	}
}

func TestSdmxAvailabilityCallsDataSources(t *testing.T) {
	source := &sdmxAvailabilitySource{
		result: &pb.SdmxAvailabilityResult{Values: []string{"country/USA"}},
	}
	query := &pb.SdmxAvailabilityQuery{
		ComponentId: "observationAbout",
		Constraints: map[string]*pb.ConstraintList{
			"variableMeasured": {Values: []string{"Count_Person"}},
		},
	}
	dispatcher := NewDispatcher(nil, datasources.NewDataSources([]datasource.DataSource{source}, nil))

	got, err := dispatcher.SdmxAvailability(context.Background(), query)
	if err != nil {
		t.Fatalf("SdmxAvailability() error = %v", err)
	}
	if diff := cmp.Diff(source.result, got, protocmp.Transform()); diff != "" {
		t.Fatalf("SdmxAvailability() response mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(query, source.got, protocmp.Transform()); diff != "" {
		t.Fatalf("SdmxAvailability() request mismatch (-want +got):\n%s", diff)
	}
}
