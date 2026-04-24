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

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
)

func TestGetNormalizedMultiEntityObservationsQuery(t *testing.T) {
	stmt, err := GetNormalizedMultiEntityObservationsQuery(
		[]string{"dcid:Amount_EconomicActivity_GrossODA"},
		[]*pbv2.ObservationDimensionConstraint{
			{
				Property: "recipient",
				Value:    &pbv2.DcidOrExpression{Dcids: []string{"country/GHA"}},
			},
			{
				Property: "donor",
				Value:    &pbv2.DcidOrExpression{Dcids: []string{"country/IRL", "country/USA"}},
			},
		},
	)
	if err != nil {
		t.Fatalf("GetNormalizedMultiEntityObservationsQuery() error = %v", err)
	}

	for _, want := range []string{
		"ts.variable_measured = @variables",
		"property = @dimension_property_0",
		"value IN UNNEST(@dimension_values_0)",
		"property = @dimension_property_1",
		"value = @dimension_values_1",
	} {
		if !strings.Contains(stmt.SQL, want) {
			t.Fatalf("SQL missing %q:\n%s", want, stmt.SQL)
		}
	}

	if got := stmt.Params["dimension_property_0"]; got != "donor" {
		t.Fatalf("dimension_property_0 = %v, want donor", got)
	}
	if got := stmt.Params["dimension_property_1"]; got != "recipient" {
		t.Fatalf("dimension_property_1 = %v, want recipient", got)
	}
}
