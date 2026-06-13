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

import "cloud.google.com/go/spanner"

// rawObservation maps the raw query result.
type rawObservation struct {
	VariableMeasured string                `spanner:"variable_measured"`
	ObservationAbout string                `spanner:"observation_about"`
	FacetId          string                `spanner:"facet_id"`
	ProvenanceID     spanner.NullString    `spanner:"provenance"`
	DatesAndValues   []*spannerObservation `spanner:"dates_and_values"`
	Facets           spanner.NullJSON      `spanner:"facets"`
}

// spannerObservation maps the STRUCT inside dates_and_values ARRAY.
type spannerObservation struct {
	Date  string `spanner:"date"`
	Value string `spanner:"str_value"`
}
