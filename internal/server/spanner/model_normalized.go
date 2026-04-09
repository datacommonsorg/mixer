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

// rawObservation represents a raw row from the query combining TimeSeries and StatVarObservation.
type rawObservation struct {
	VariableMeasured string `spanner:"variable_measured"`
	Id               string `spanner:"id"`
	Provenance       string `spanner:"provenance"`
	Date             string `spanner:"date"`
	Value            string `spanner:"value"`
}

// rawAttr represents a raw row from the TimeSeriesAttribute table.
type rawAttr struct {
	Id       string `spanner:"id"`
	Property string `spanner:"property"`
	Value    string `spanner:"value"`
}
