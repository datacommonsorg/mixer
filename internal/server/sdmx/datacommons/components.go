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

package datacommons

type ComponentKind string

const (
	ComponentKindDimension ComponentKind = "dimension"
	ComponentKindMeasure   ComponentKind = "measure"
	ComponentKindAttribute ComponentKind = "attribute"

	ComponentVariableMeasured  = "variableMeasured"
	ComponentTimePeriod        = "TIME_PERIOD"
	ComponentObservationAbout  = "observationAbout"
	ComponentObservationValue  = "OBS_VALUE"
	ComponentUnit              = "unit"
	ComponentMeasurementMethod = "measurementMethod"
	ComponentObservationPeriod = "observationPeriod"
	ComponentProvenance        = "provenance"
	ComponentScalingFactor     = "scalingFactor"

	// FallbackNotAvailable is used across datasets to represent missing constraints.
	FallbackNotAvailable = "NotApplicable"
)

type DataComponent struct {
	ID   string
	Kind ComponentKind
}

// DataComponents is Data Commons observation dataflow component metadata.
// The order is used by SDMX-CSV formatting.
var DataComponents = []DataComponent{
	{ID: ComponentVariableMeasured, Kind: ComponentKindDimension},
	{ID: ComponentObservationAbout, Kind: ComponentKindDimension},
	{ID: ComponentUnit, Kind: ComponentKindDimension},
	{ID: ComponentMeasurementMethod, Kind: ComponentKindDimension},
	{ID: ComponentObservationPeriod, Kind: ComponentKindDimension},
	{ID: ComponentProvenance, Kind: ComponentKindDimension},
	{ID: ComponentTimePeriod, Kind: ComponentKindDimension},
	{ID: ComponentObservationValue, Kind: ComponentKindMeasure},
	{ID: ComponentScalingFactor, Kind: ComponentKindAttribute},
}
