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
	"github.com/google/go-cmp/cmp"
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
