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

package format

import (
	"fmt"

	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
)

func DataComponentsFromShape(shape *sdmxpb.SdmxDataShape) ([]datacommons.DataComponent, error) {
	if shape == nil || len(shape.GetComponents()) == 0 {
		return nil, fmt.Errorf("SDMX data shape is required")
	}
	components := make([]datacommons.DataComponent, 0, len(shape.GetComponents()))
	for _, component := range shape.GetComponents() {
		if component == nil {
			return nil, fmt.Errorf("SDMX data shape contains nil component")
		}
		kind, ok := componentKind(component.GetKind())
		if !ok {
			return nil, fmt.Errorf("unsupported SDMX component kind %q for %q", component.GetKind(), component.GetId())
		}
		components = append(components, datacommons.DataComponent{
			ID:   component.GetId(),
			Kind: kind,
		})
	}
	return components, nil
}

func SdmxComponentValue(
	component datacommons.DataComponent,
	series *sdmxpb.SdmxTimeSeries,
	point *sdmxpb.SdmxDataPoint,
) string {
	if series == nil {
		return ""
	}
	switch component.Kind {
	case datacommons.ComponentKindDimension:
		if component.ID == datacommons.ComponentTimePeriod {
			if point == nil {
				return ""
			}
			return point.GetTimePeriod()
		}
		return series.GetDimensions()[component.ID]
	case datacommons.ComponentKindMeasure:
		if component.ID == datacommons.ComponentObservationValue {
			if point == nil {
				return ""
			}
			return point.GetObservationValue()
		}
		return ""
	case datacommons.ComponentKindAttribute:
		return series.GetAttributes()[component.ID]
	default:
		return ""
	}
}

func componentKind(kind sdmxpb.SdmxComponentKind) (datacommons.ComponentKind, bool) {
	switch kind {
	case sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_DIMENSION:
		return datacommons.ComponentKindDimension, true
	case sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_MEASURE:
		return datacommons.ComponentKindMeasure, true
	case sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_ATTRIBUTE:
		return datacommons.ComponentKindAttribute, true
	default:
		return "", false
	}
}
