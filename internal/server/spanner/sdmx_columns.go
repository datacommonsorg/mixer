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

import "github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"

var sdmxStaticDataFilterColumns = map[string]string{
	datacommons.ComponentMeasurementMethod: "measurement_method",
	datacommons.ComponentObservationPeriod: "observation_period",
	datacommons.ComponentProvenance:        "provenance",
	datacommons.ComponentUnit:              "unit",
}

func sdmxStaticDataFilterColumn(componentID string) (string, bool) {
	spannerColumn, ok := sdmxStaticDataFilterColumns[componentID]
	return spannerColumn, ok
}

func sdmxAvailabilityValueColumn(componentID string) (string, bool) {
	switch componentID {
	case datacommons.ComponentVariableMeasured:
		return "variable_measured", true
	case datacommons.ComponentObservationAbout:
		return "entity1", true
	default:
		return sdmxStaticDataFilterColumn(componentID)
	}
}
