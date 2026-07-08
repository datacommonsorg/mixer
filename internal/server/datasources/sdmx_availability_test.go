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

package datasources

import (
	"context"
	"testing"

	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type availabilityDataSource struct {
	datasource.DataSource
	id     string
	values []string
}

func (s *availabilityDataSource) Type() datasource.DataSourceType {
	return datasource.TypeMock
}

func (s *availabilityDataSource) Id() string {
	return s.id
}

func (s *availabilityDataSource) SdmxAvailability(ctx context.Context, req *sdmxpb.SdmxAvailabilityQuery) (*sdmxpb.SdmxAvailabilityResult, error) {
	return &sdmxpb.SdmxAvailabilityResult{Values: s.values}, nil
}

type sdmxDataSource struct {
	datasource.DataSource
	id     string
	result *sdmxpb.SdmxDataResult
}

func (s *sdmxDataSource) Type() datasource.DataSourceType {
	return datasource.TypeMock
}

func (s *sdmxDataSource) Id() string {
	return s.id
}

func (s *sdmxDataSource) SdmxData(ctx context.Context, req *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error) {
	return s.result, nil
}

func TestSdmxAvailabilityMergesValues(t *testing.T) {
	ds := NewDataSources([]datasource.DataSource{
		&availabilityDataSource{id: "one", values: []string{"geoId/06", "country/USA", ""}},
		&availabilityDataSource{id: "two", values: []string{"country/USA", "geoId/12"}},
	}, nil)

	got, err := ds.SdmxAvailability(context.Background(), &sdmxpb.SdmxAvailabilityQuery{})
	if err != nil {
		t.Fatalf("SdmxAvailability() error = %v", err)
	}
	want := []string{"country/USA", "geoId/06", "geoId/12"}
	if diff := cmp.Diff(want, got.GetValues()); diff != "" {
		t.Fatalf("SdmxAvailability() values mismatch (-want +got):\n%s", diff)
	}
}

func TestSdmxDataMergesIdenticalShapes(t *testing.T) {
	shape := testSdmxShape([]string{datacommons.ComponentObservationAbout})
	ds := NewDataSources([]datasource.DataSource{
		&sdmxDataSource{
			id: "one",
			result: &sdmxpb.SdmxDataResult{
				Shape: shape,
				Series: []*sdmxpb.SdmxTimeSeries{
					{Dimensions: map[string]string{datacommons.ComponentVariableMeasured: "Count_Person"}},
				},
			},
		},
		&sdmxDataSource{
			id: "two",
			result: &sdmxpb.SdmxDataResult{
				Shape: shape,
				Series: []*sdmxpb.SdmxTimeSeries{
					{Dimensions: map[string]string{datacommons.ComponentVariableMeasured: "Count_Household"}},
				},
			},
		},
	}, nil)

	got, err := ds.SdmxData(context.Background(), &sdmxpb.SdmxDataQuery{})
	if err != nil {
		t.Fatalf("SdmxData() error = %v", err)
	}
	if !sameSdmxShape(got.GetShape(), shape) {
		t.Fatalf("SdmxData() shape = %v, want %v", got.GetShape(), shape)
	}
	if len(got.GetSeries()) != 2 {
		t.Fatalf("len(SdmxData().Series) = %d, want 2", len(got.GetSeries()))
	}
}

func TestSdmxDataRejectsIncompatibleShapes(t *testing.T) {
	ds := NewDataSources([]datasource.DataSource{
		&sdmxDataSource{
			id: "single",
			result: &sdmxpb.SdmxDataResult{
				Shape: testSdmxShape([]string{datacommons.ComponentObservationAbout}),
			},
		},
		&sdmxDataSource{
			id: "multi",
			result: &sdmxpb.SdmxDataResult{
				Shape: testSdmxShape([]string{"destinationCountry", "sourceCountry"}),
			},
		},
	}, nil)

	_, err := ds.SdmxData(context.Background(), &sdmxpb.SdmxDataQuery{})
	if err == nil {
		t.Fatal("SdmxData() error = nil, want error")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("SdmxData() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
	}
	if got, want := status.Convert(err).Message(), "SdmxData: incompatible data shapes"; got != want {
		t.Fatalf("SdmxData() message = %q, want %q", got, want)
	}
}

func TestSdmxDataRejectsSeriesWithoutShape(t *testing.T) {
	ds := NewDataSources([]datasource.DataSource{
		&sdmxDataSource{
			id: "missing-shape",
			result: &sdmxpb.SdmxDataResult{
				Series: []*sdmxpb.SdmxTimeSeries{
					{Dimensions: map[string]string{datacommons.ComponentVariableMeasured: "Count_Person"}},
				},
			},
		},
	}, nil)

	_, err := ds.SdmxData(context.Background(), &sdmxpb.SdmxDataQuery{})
	if err == nil {
		t.Fatal("SdmxData() error = nil, want error")
	}
	if got, want := err.Error(), "SdmxData: data result missing shape"; got != want {
		t.Fatalf("SdmxData() error = %q, want %q", got, want)
	}
}

func testSdmxShape(entityDimensions []string) *sdmxpb.SdmxDataShape {
	components := datacommons.DataComponentsForEntityDimensions(entityDimensions)
	shape := &sdmxpb.SdmxDataShape{
		Components: make([]*sdmxpb.SdmxComponent, 0, len(components)),
	}
	for _, component := range components {
		shape.Components = append(shape.Components, &sdmxpb.SdmxComponent{
			Id:   component.ID,
			Kind: testProtoComponentKind(component.Kind),
		})
	}
	return shape
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
