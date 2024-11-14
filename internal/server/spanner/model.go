// Copyright 2024 Google LLC
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

// Model objects related to the spanner graph database.
package spanner

// Edge struct represents a single row in the Edge table.
type Edge struct {
	SubjectID   string `spanner:"subject_id"`
	Predicate   string `spanner:"predicate"`
	ObjectID    string `spanner:"object_id"`
	ObjectValue string `spanner:"object_value"`
	Provenance  string `spanner:"provenance"`
}

// StatVarObservation struct represents a single row in the StatVarObservation table.
type StatVarObservation struct {
	VariableMeasured  string `spanner:"variable_measured"`
	ObservationAbout  string `spanner:"observation_about"`
	ObservationDate   string `spanner:"observation_date"`
	Value             string `spanner:"value"`
	Provenance        string `spanner:"provenance"`
	ObservationPeriod string `spanner:"observation_period"`
	MeasurementMethod string `spanner:"measurement_method"`
	Unit              string `spanner:"unit"`
	ScalingFactor     string `spanner:"scaling_factor"`
}

// SpannerConfig struct to hold the YAML configuration to a spanner database.
type SpannerConfig struct {
	Project  string `yaml:"project"`
	Instance string `yaml:"instance"`
	Database string `yaml:"database"`
}
