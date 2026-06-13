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
	"testing"

	cloudspanner "cloud.google.com/go/spanner"
)

func TestReconstructObservationsUsesStoredFacetID(t *testing.T) {
	observations := reconstructObservations([]*rawObservation{
		{
			VariableMeasured: "Count_Person",
			ObservationAbout: "geoId/06",
			FacetId:          "stored-facet-id",
			DatesAndValues: []*spannerObservation{
				{Date: "2020", Value: "1"},
			},
			Facets: cloudspanner.NullJSON{
				Value: map[string]interface{}{
					"facetId":    "json-facet-id",
					"importName": "test_import",
				},
				Valid: true,
			},
		},
	})

	if got, want := len(observations), 1; got != want {
		t.Fatalf("len(observations) = %d, want %d", got, want)
	}
	if got, want := observations[0].FacetId, "stored-facet-id"; got != want {
		t.Fatalf("observations[0].FacetId = %q, want %q", got, want)
	}
	if got, want := observations[0].ImportName, "test_import"; got != want {
		t.Fatalf("observations[0].ImportName = %q, want %q", got, want)
	}
}
