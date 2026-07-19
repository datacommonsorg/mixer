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
	"errors"
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
	err    error
}

type sdmxDataConstraintSource struct {
	datasource.DataSource
	got *sdmxpb.SdmxDataQuery
	err error
}

func (s *sdmxDataConstraintSource) Type() datasource.DataSourceType {
	return datasource.TypeMock
}

func (s *sdmxDataConstraintSource) Id() string {
	return "sdmx-data-constraint-test"
}

func (s *sdmxDataConstraintSource) SdmxData(ctx context.Context, req *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error) {
	s.got = req
	return &sdmxpb.SdmxDataResult{}, s.err
}

func (s *sdmxAvailabilitySource) Type() datasource.DataSourceType {
	return datasource.TypeMock
}

func (s *sdmxAvailabilitySource) Id() string {
	return "availability-test"
}

func (s *sdmxAvailabilitySource) SdmxAvailability(ctx context.Context, req *sdmxpb.SdmxAvailabilityQuery) (*sdmxpb.SdmxAvailabilityResult, error) {
	s.got = req
	return s.result, s.err
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

func TestSdmxDataForwardsConstraintsWithoutValidation(t *testing.T) {
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
	}{
		{name: "missing variable measured"},
		{
			name: "property on known non observation component",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured": sdmxComponentConstraint("Count_Person"),
				"unit":             propertyConstraint,
			},
		},
		{
			name: "latest mixed with explicit date",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured": sdmxComponentConstraint("Count_Person"),
				"TIME_PERIOD":      sdmxComponentConstraint("LATEST", "2020"),
			},
		},
		{
			name: "unsupported operator",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured": {
					Predicates: []*sdmxpb.SdmxPredicate{{Operator: sdmxpb.SdmxOperator(1), Value: "Count_Person"}},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sourceErr := errors.New("source validation failed")
			source := &sdmxDataConstraintSource{err: sourceErr}
			dispatcher := NewDispatcher(nil, datasources.NewDataSources([]datasource.DataSource{source}, nil))
			query := &sdmxpb.SdmxDataQuery{Constraints: tc.constraints}
			_, err := dispatcher.SdmxData(context.Background(), query)
			if !errors.Is(err, sourceErr) {
				t.Fatalf("SdmxData() error = %v, want %v", err, sourceErr)
			}
			if diff := cmp.Diff(query, source.got, protocmp.Transform()); diff != "" {
				t.Fatalf("SdmxData() request mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSdmxAvailabilityForwardsConstraintsWithoutValidation(t *testing.T) {
	propertyConstraint := &sdmxpb.SdmxComponentConstraint{
		PropertyConstraints: map[string]*sdmxpb.SdmxPropertyConstraint{
			"typeOf": {Predicates: []*sdmxpb.SdmxPredicate{{Value: "County"}}},
		},
	}
	for _, tc := range []struct {
		name  string
		query *sdmxpb.SdmxAvailabilityQuery
	}{
		{
			name:  "missing variable measured",
			query: &sdmxpb.SdmxAvailabilityQuery{ComponentId: "observationAbout"},
		},
		{
			name: "selected time period",
			query: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "TIME_PERIOD",
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
					"variableMeasured": sdmxComponentConstraint("Count_Person"),
				},
			},
		},
		{
			name: "filtered time period",
			query: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "observationAbout",
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
					"variableMeasured": sdmxComponentConstraint("Count_Person"),
					"TIME_PERIOD":      sdmxComponentConstraint("2020"),
				},
			},
		},
		{
			name: "property constraint",
			query: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "source",
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
					"source": propertyConstraint,
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sourceErr := errors.New("source validation failed")
			source := &sdmxAvailabilitySource{err: sourceErr}
			dispatcher := NewDispatcher(nil, datasources.NewDataSources([]datasource.DataSource{source}, nil))
			_, err := dispatcher.SdmxAvailability(context.Background(), tc.query)
			if !errors.Is(err, sourceErr) {
				t.Fatalf("SdmxAvailability() error = %v, want %v", err, sourceErr)
			}
			if diff := cmp.Diff(tc.query, source.got, protocmp.Transform()); diff != "" {
				t.Fatalf("SdmxAvailability() request mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
