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

	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
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

type sdmxAvailabilitySource struct {
	datasource.DataSource
	got    *sdmxpb.SdmxAvailabilityQuery
	result *sdmxpb.SdmxAvailabilityResult
}

type sdmxDataConstraintSource struct {
	datasource.DataSource
	got *sdmxpb.SdmxDataQuery
}

func (s *sdmxDataConstraintSource) Type() datasource.DataSourceType {
	return datasource.TypeMock
}

func (s *sdmxDataConstraintSource) Id() string {
	return "sdmx-data-constraint-test"
}

func (s *sdmxDataConstraintSource) SdmxData(ctx context.Context, req *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error) {
	s.got = req
	return &sdmxpb.SdmxDataResult{}, nil
}

func (s *sdmxAvailabilitySource) Type() datasource.DataSourceType {
	return datasource.TypeMock
}

func (s *sdmxAvailabilitySource) Id() string {
	return "availability-test"
}

func (s *sdmxAvailabilitySource) SdmxAvailability(ctx context.Context, req *sdmxpb.SdmxAvailabilityQuery) (*sdmxpb.SdmxAvailabilityResult, error) {
	s.got = req
	return s.result, nil
}

func TestSdmxAvailabilityBackendUnimplementedWithoutSources(t *testing.T) {
	dispatcher := NewDispatcher(nil, nil)
	got, err := dispatcher.SdmxAvailability(
		context.Background(),
		&sdmxpb.SdmxAvailabilityQuery{
			ComponentId: "observationAbout",
			Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured": sdmxComponentConstraint("Count_Person"),
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
		result: &sdmxpb.SdmxAvailabilityResult{Values: []string{"country/USA"}},
	}
	query := &sdmxpb.SdmxAvailabilityQuery{
		ComponentId: "observationAbout",
		Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("Count_Person"),
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

func TestSdmxDataPropertyConstraintsCallsDataSources(t *testing.T) {
	componentConstraint := &sdmxpb.SdmxComponentConstraint{
		PropertyConstraints: map[string]*sdmxpb.SdmxPropertyConstraint{
			"containedInPlace": {
				Predicates: []*sdmxpb.SdmxPredicate{{Value: "country/USA"}},
				Transitive: true,
			},
			"typeOf": {Predicates: []*sdmxpb.SdmxPredicate{{Value: "County"}}},
		},
	}
	query := &sdmxpb.SdmxDataQuery{
		Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("Count_Person"),
			"observationAbout": componentConstraint,
		},
	}
	source := &sdmxDataConstraintSource{}
	dispatcher := NewDispatcher(nil, datasources.NewDataSources([]datasource.DataSource{source}, nil))
	if _, err := dispatcher.SdmxData(context.Background(), query); err != nil {
		t.Fatalf("SdmxData() error = %v", err)
	}
	if diff := cmp.Diff(query, source.got, protocmp.Transform()); diff != "" {
		t.Fatalf("SdmxData() request mismatch (-want +got):\n%s", diff)
	}
}

func TestSdmxDataTimeConstraintsCallDataSources(t *testing.T) {
	query := &sdmxpb.SdmxDataQuery{
		Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("Count_Person"),
			"TIME_PERIOD":      sdmxComponentConstraint("2020", "2022"),
		},
	}
	source := &sdmxDataConstraintSource{}
	dispatcher := NewDispatcher(nil, datasources.NewDataSources([]datasource.DataSource{source}, nil))
	if _, err := dispatcher.SdmxData(context.Background(), query); err != nil {
		t.Fatalf("SdmxData() error = %v", err)
	}
	if diff := cmp.Diff(query, source.got, protocmp.Transform()); diff != "" {
		t.Fatalf("SdmxData() request mismatch (-want +got):\n%s", diff)
	}
}

func TestSdmxDataConstraintValidationBeforeSources(t *testing.T) {
	propertyConstraint := &sdmxpb.SdmxComponentConstraint{
		PropertyConstraints: map[string]*sdmxpb.SdmxPropertyConstraint{
			"containedInPlace": {
				Predicates: []*sdmxpb.SdmxPredicate{{Value: "country/USA"}},
				Transitive: true,
			},
			"typeOf": {Predicates: []*sdmxpb.SdmxPredicate{{Value: "County"}}},
		},
	}
	for _, tc := range []struct {
		name        string
		constraints map[string]*sdmxpb.SdmxComponentConstraint
		wantCode    codes.Code
	}{
		{name: "missing variable measured", wantCode: codes.InvalidArgument},
		{
			name: "property on known non observation component",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured": sdmxComponentConstraint("Count_Person"),
				"unit":             propertyConstraint,
			},
			wantCode: codes.Unimplemented,
		},
		{
			name: "latest mixed with explicit date",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured": sdmxComponentConstraint("Count_Person"),
				"TIME_PERIOD":      sdmxComponentConstraint("LATEST", "2020"),
			},
			wantCode: codes.InvalidArgument,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &sdmxDataConstraintSource{}
			dispatcher := NewDispatcher(nil, datasources.NewDataSources([]datasource.DataSource{source}, nil))
			_, err := dispatcher.SdmxData(context.Background(), &sdmxpb.SdmxDataQuery{Constraints: tc.constraints})
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("SdmxData() code = %v, want %v; err = %v", got, tc.wantCode, err)
			}
			if source.got != nil {
				t.Fatalf("SdmxData() called source with %v", source.got)
			}
		})
	}
}

func TestSdmxAvailabilityRequiresVariableMeasuredBeforeSources(t *testing.T) {
	source := &sdmxAvailabilitySource{}
	dispatcher := NewDispatcher(nil, datasources.NewDataSources([]datasource.DataSource{source}, nil))
	_, err := dispatcher.SdmxAvailability(context.Background(), &sdmxpb.SdmxAvailabilityQuery{ComponentId: "observationAbout"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("SdmxAvailability() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
	}
	if source.got != nil {
		t.Fatalf("SdmxAvailability() called source with %v", source.got)
	}
}

func TestSdmxAvailabilityRejectsTimePeriodBeforeSources(t *testing.T) {
	for _, query := range []*sdmxpb.SdmxAvailabilityQuery{
		{
			ComponentId: "TIME_PERIOD",
			Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured": sdmxComponentConstraint("Count_Person"),
			},
		},
		{
			ComponentId: "observationAbout",
			Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured": sdmxComponentConstraint("Count_Person"),
				"TIME_PERIOD":      sdmxComponentConstraint("2020"),
			},
		},
	} {
		source := &sdmxAvailabilitySource{}
		dispatcher := NewDispatcher(nil, datasources.NewDataSources([]datasource.DataSource{source}, nil))
		_, err := dispatcher.SdmxAvailability(context.Background(), query)
		if status.Code(err) != codes.Unimplemented {
			t.Fatalf("SdmxAvailability() code = %v, want %v; err = %v", status.Code(err), codes.Unimplemented, err)
		}
		if source.got != nil {
			t.Fatalf("SdmxAvailability() called source with %v", source.got)
		}
	}
}

func TestSdmxAvailabilityPropertyConstraintsUnimplemented(t *testing.T) {
	_, err := NewDispatcher(nil, nil).SdmxAvailability(context.Background(), &sdmxpb.SdmxAvailabilityQuery{
		ComponentId: "source",
		Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"source": {
				PropertyConstraints: map[string]*sdmxpb.SdmxPropertyConstraint{
					"typeOf": {Predicates: []*sdmxpb.SdmxPredicate{{Value: "County"}}},
				},
			},
		},
	})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("code = %v, want %v; err = %v", status.Code(err), codes.Unimplemented, err)
	}
	if got, want := status.Convert(err).Message(), "SDMX property constraints are not implemented for availability yet"; got != want {
		t.Fatalf("message = %q, want %q", got, want)
	}
}

func TestSdmxUnsupportedOperatorUnimplemented(t *testing.T) {
	componentConstraint := sdmxComponentConstraint("country/USA")
	componentConstraint.Predicates[0].Operator = sdmxpb.SdmxOperator(1)

	_, err := NewDispatcher(nil, nil).SdmxData(context.Background(), &sdmxpb.SdmxDataQuery{
		Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"source": componentConstraint,
		},
	})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("code = %v, want %v; err = %v", status.Code(err), codes.Unimplemented, err)
	}
	if got, want := status.Convert(err).Message(), "SDMX operators other than EQ are not implemented yet"; got != want {
		t.Fatalf("message = %q, want %q", got, want)
	}
}
