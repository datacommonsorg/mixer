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
	"strings"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/util"
)

func TestValidateMultiEntityObservationRequest(t *testing.T) {
	validDimension := &pbv2.ObservationDimensionConstraint{
		Property: "recipient",
		Value: &pbv2.DcidOrExpression{
			Dcids: []string{"country/GHA"},
		},
	}

	for _, c := range []struct {
		name    string
		req     *pbv2.ObservationRequest
		wantErr string
	}{
		{
			name: "valid",
			req: &pbv2.ObservationRequest{
				Variable:              &pbv2.DcidOrExpression{Dcids: []string{"dcid:Amount_EconomicActivity_GrossODA"}},
				ObservationDimensions: []*pbv2.ObservationDimensionConstraint{validDimension},
				NodeProperties:        []string{"name"},
			},
		},
		{
			name: "entity and dimensions",
			req: &pbv2.ObservationRequest{
				Variable:              &pbv2.DcidOrExpression{Dcids: []string{"dcid:Amount_EconomicActivity_GrossODA"}},
				Entity:                &pbv2.DcidOrExpression{Dcids: []string{"country/GHA"}},
				ObservationDimensions: []*pbv2.ObservationDimensionConstraint{validDimension},
			},
			wantErr: "only one of entity and observation_dimensions",
		},
		{
			name: "variable formula",
			req: &pbv2.ObservationRequest{
				Variable:              &pbv2.DcidOrExpression{Formula: "Count_Person + Count_Farm"},
				ObservationDimensions: []*pbv2.ObservationDimensionConstraint{validDimension},
			},
			wantErr: "variable.dcids must be specified",
		},
		{
			name: "dimension expression",
			req: &pbv2.ObservationRequest{
				Variable: &pbv2.DcidOrExpression{Dcids: []string{"dcid:Amount_EconomicActivity_GrossODA"}},
				ObservationDimensions: []*pbv2.ObservationDimensionConstraint{
					{
						Property: "recipient",
						Value:    &pbv2.DcidOrExpression{Expression: "country/GHA<-containedInPlace+{typeOf:Country}"},
					},
				},
			},
			wantErr: "value.dcids must be specified",
		},
		{
			name: "duplicate dimension",
			req: &pbv2.ObservationRequest{
				Variable: &pbv2.DcidOrExpression{Dcids: []string{"dcid:Amount_EconomicActivity_GrossODA"}},
				ObservationDimensions: []*pbv2.ObservationDimensionConstraint{
					validDimension,
					{
						Property: "recipient",
						Value:    &pbv2.DcidOrExpression{Dcids: []string{"country/NER"}},
					},
				},
			},
			wantErr: "duplicate observation_dimensions property",
		},
		{
			name: "empty node property",
			req: &pbv2.ObservationRequest{
				Variable:              &pbv2.DcidOrExpression{Dcids: []string{"dcid:Amount_EconomicActivity_GrossODA"}},
				ObservationDimensions: []*pbv2.ObservationDimensionConstraint{validDimension},
				NodeProperties:        []string{""},
			},
			wantErr: "node_properties cannot contain empty values",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := validateMultiEntityObservationRequest(c.req)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("validateMultiEntityObservationRequest() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("validateMultiEntityObservationRequest() = %v, want error containing %q", err, c.wantErr)
			}
		})
	}
}

func TestMultiEntityObservationsToResponse(t *testing.T) {
	resp := multiEntityObservationsToResponse(
		[]string{"dcid:Amount_EconomicActivity_GrossODA"},
		[]*multiEntityObservation{
			{
				VariableMeasured: "dcid:Amount_EconomicActivity_GrossODA",
				Provenance:       "provenance/OECD",
				Attributes: []*spannerAttribute{
					{Property: "recipient", Value: "country/GHA"},
					{Property: "unit", Value: "dcs:USDollar"},
					{Property: "measurementMethod", Value: "dcid:OECD_UnvalidatedValue"},
					{Property: "donor", Value: "country/IRL"},
				},
				Observations: TimeSeries{
					{Date: "1998", Value: "520000"},
					{
						Date:  "1997",
						Value: "510000",
						Attributes: []*spannerAttribute{
							{Property: "footnote", Value: "Preliminary data for early Q1"},
						},
					},
				},
			},
		},
	)

	varObs := resp.GetByVariable()["dcid:Amount_EconomicActivity_GrossODA"]
	if varObs == nil || len(varObs.GetMultiEntityObservations()) != 1 {
		t.Fatalf("multi_entity_observations size = %d, want 1", len(varObs.GetMultiEntityObservations()))
	}
	obs := varObs.GetMultiEntityObservations()[0]
	if got := len(obs.GetObservationDimensions()); got != 2 {
		t.Fatalf("observation_dimensions size = %d, want 2", got)
	}
	if obs.GetObservationDimensions()[0].GetProperty() != "donor" || obs.GetObservationDimensions()[1].GetProperty() != "recipient" {
		t.Fatalf("observation_dimensions = %v, want donor then recipient", obs.GetObservationDimensions())
	}

	wantFacet := &pb.Facet{
		ImportName:        "provenance/OECD",
		MeasurementMethod: "dcid:OECD_UnvalidatedValue",
		Unit:              "dcs:USDollar",
	}
	wantFacetID := util.GetFacetID(wantFacet)
	facet := obs.GetObservation().GetOrderedFacets()[0]
	if facet.GetFacetId() != wantFacetID {
		t.Fatalf("facet_id = %q, want %q", facet.GetFacetId(), wantFacetID)
	}
	if facet.GetEarliestDate() != "1997" || facet.GetLatestDate() != "1998" {
		t.Fatalf("date range = %s-%s, want 1997-1998", facet.GetEarliestDate(), facet.GetLatestDate())
	}
	if resp.GetFacets()[wantFacetID].GetUnit() != "dcs:USDollar" {
		t.Fatalf("facet unit = %q, want dcs:USDollar", resp.GetFacets()[wantFacetID].GetUnit())
	}
	if resp.GetFacets()[wantFacetID].GetMeasurementMethod() != "dcid:OECD_UnvalidatedValue" {
		t.Fatalf("facet measurement_method = %q, want dcid:OECD_UnvalidatedValue", resp.GetFacets()[wantFacetID].GetMeasurementMethod())
	}
	pointAttrs := facet.GetObservations()[0].GetObservationAttributes()
	if len(pointAttrs) != 1 || pointAttrs[0].GetProperty() != "footnote" {
		t.Fatalf("point attributes = %v, want footnote", pointAttrs)
	}
}

func TestMultiEntityFacetPrefersStoredFacetID(t *testing.T) {
	facetID, facet := multiEntityFacet(&multiEntityObservation{
		Provenance: "provenance/OECD",
		Attributes: []*spannerAttribute{
			{Property: "facetId", Value: "storedFacet"},
			{Property: "unit", Value: "dcs:USDollar"},
		},
	})

	if facetID != "storedFacet" {
		t.Fatalf("facetID = %q, want storedFacet", facetID)
	}
	if facet.GetUnit() != "dcs:USDollar" {
		t.Fatalf("facet unit = %q, want dcs:USDollar", facet.GetUnit())
	}
}
