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

package spanner

import (
	"context"
	"testing"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
)



// Tests moved from golden/datasource_test.go

func TestSpannerObservation_ExpressionExpansion(t *testing.T) {
	ctx := context.Background()

	client := &mockSpannerClient{
		getNodeEdgesRes: map[string][]*Edge{
			"geoId/06": {
				{Value: "geoId/06002", Predicate: "linkedContainedInPlace"},
			},
		},
		getObservationsRes: []*Observation{
			{
				VariableMeasured: "Count_Person",
				ObservationAbout: "geoId/06001",
				Observations: []*DateValue{
					{Date: "2020", Value: "12345"},
				},
			},
			{
				VariableMeasured: "Count_Person",
				ObservationAbout: "geoId/06002",
				Observations: []*DateValue{
					{Date: "2020", Value: "67890"},
				},
			},
		},
	}

	ds := NewSpannerDataSource(client, nil, nil, false)

	req := &pbv2.ObservationRequest{
		Variable: &pbv2.DcidOrExpression{Dcids: []string{"Count_Person"}},
		Entity:   &pbv2.DcidOrExpression{Expression: "geoId/06<-containedInPlace+{typeOf:County}"},
		Select:   []string{"variable", "entity", "value"},
	}

	remoteDCIDs := []string{"geoId/06001"}
	ctxWithRemote := context.WithValue(ctx, dispatcher.RelationExpressionExpandedEntities, remoteDCIDs)

	resp, err := ds.Observation(ctxWithRemote, req)
	if err != nil {
		t.Fatalf("Observation failed: %v", err)
	}

	if resp == nil {
		t.Fatal("Expected non-nil response")
		return
	}

	byVariable := resp.ByVariable
	if byVariable == nil {
		t.Fatal("Expected ByVariable to be populated")
	}
	countPerson, ok := byVariable["Count_Person"]
	if !ok {
		t.Fatal("Expected Count_Person in response")
	}
	byEntity := countPerson.ByEntity
	if byEntity == nil {
		t.Fatal("Expected ByEntity to be populated")
	}

	if _, ok := byEntity["geoId/06001"]; !ok {
		t.Errorf("Expected data for geoId/06001 (remote place)")
	}
	if _, ok := byEntity["geoId/06002"]; !ok {
		t.Errorf("Expected data for geoId/06002 (local place)")
	}
}

func TestSpannerObservation_ExpressionExpansion_Fallback(t *testing.T) {
	ctx := context.Background()

	client := &mockSpannerClient{
		getObservationsContainedInPlaceRes: []*Observation{
			{
				VariableMeasured: "Count_Person",
				ObservationAbout: "geoId/06002",
				Observations: []*DateValue{
					{Date: "2020", Value: "67890"},
				},
			},
		},
	}

	ds := NewSpannerDataSource(client, nil, nil, false)

	req := &pbv2.ObservationRequest{
		Variable: &pbv2.DcidOrExpression{Dcids: []string{"Count_Person"}},
		Entity:   &pbv2.DcidOrExpression{Expression: "geoId/06<-containedInPlace+{typeOf:County}"},
		Select:   []string{"variable", "entity", "value"},
	}

	resp, err := ds.Observation(ctx, req)
	if err != nil {
		t.Fatalf("Observation failed: %v", err)
	}

	if resp == nil {
		t.Fatal("Expected non-nil response")
		return
	}

	byVariable := resp.ByVariable
	countPerson := byVariable["Count_Person"]
	byEntity := countPerson.ByEntity

	if _, ok := byEntity["geoId/06002"]; !ok {
		t.Errorf("Expected data for geoId/06002 (local place)")
	}
}

func TestSpannerObservation_NoExpression(t *testing.T) {
	ctx := context.Background()

	client := &mockSpannerClient{
		getObservationsRes: []*Observation{
			{
				VariableMeasured: "Count_Person",
				ObservationAbout: "geoId/06",
				Observations: []*DateValue{
					{Date: "2020", Value: "12345"},
				},
			},
		},
	}

	ds := NewSpannerDataSource(client, nil, nil, false)

	req := &pbv2.ObservationRequest{
		Variable: &pbv2.DcidOrExpression{Dcids: []string{"Count_Person"}},
		Entity:   &pbv2.DcidOrExpression{Dcids: []string{"geoId/06"}},
		Select:   []string{"variable", "entity", "value"},
	}

	resp, err := ds.Observation(ctx, req)
	if err != nil {
		t.Fatalf("Observation failed: %v", err)
	}

	if resp == nil {
		t.Fatal("Expected non-nil response")
		return
	}

	byVariable := resp.ByVariable
	countPerson := byVariable["Count_Person"]
	byEntity := countPerson.ByEntity

	if _, ok := byEntity["geoId/06"]; !ok {
		t.Errorf("Expected data for geoId/06")
	}
}
