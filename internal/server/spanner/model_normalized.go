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

// rawObservation represents a row from the coalesced query.
type rawObservation struct {
	VariableMeasured string                `spanner:"variable_measured"`
	DatesAndValues   []*spannerObservation `spanner:"dates_and_values"`
	Attributes       []*spannerAttribute   `spanner:"attributes"`
}

type rawMultiEntityObservation struct {
	VariableMeasured string                `spanner:"variable_measured"`
	Provenance       string                `spanner:"provenance"`
	DatesAndValues   []*spannerObservation `spanner:"dates_and_values"`
	Attributes       []*spannerAttribute   `spanner:"attributes"`
}

type multiEntityObservation struct {
	VariableMeasured string
	Provenance       string
	Observations     TimeSeries
	Attributes       []*spannerAttribute
}

// spannerObservation represents the STRUCT returned in dates_and_values array.
type spannerObservation struct {
	Date       string              `spanner:"date"`
	Value      string              `spanner:"value"`
	Attributes []*spannerAttribute `spanner:"attributes"`
}

// spannerAttribute represents the STRUCT returned in attributes array.
type spannerAttribute struct {
	Property string `spanner:"property"`
	Value    string `spanner:"value"`
}
