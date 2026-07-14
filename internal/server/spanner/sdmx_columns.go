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

func sdmxStaticDataFilterColumn(componentID string) (string, bool) {
	switch componentID {
	case datacommons.ComponentMeasurementMethod:
		return "measurement_method", true
	case datacommons.ComponentObservationPeriod:
		return "observation_period", true
	case datacommons.ComponentProvenance:
		return "provenance", true
	case datacommons.ComponentUnit:
		return "unit", true
	default:
		return "", false
	}
}

func sdmxDataFilterColumn(componentID string, entitySlotByObservationProperty map[string]string) (string, bool) {
	if entitySlot, ok := entitySlotByObservationProperty[componentID]; ok {
		return entitySlot, true
	}
	return sdmxStaticDataFilterColumn(componentID)
}
