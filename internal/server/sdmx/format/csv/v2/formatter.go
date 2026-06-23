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

package csvv2

import (
	"bytes"
	"encoding/csv"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
)

const (
	csvStructureDataflow = "dataflow"
	csvActionInformation = "I"
)

// CSVFormatter implements Formatter for SDMX-CSV 2.0 data messages.
type CSVFormatter struct {
	StructureID string
}

// Format converts observations into a complete SDMX-CSV 2.0 payload.
func (f *CSVFormatter) Format(obs []*pb.SdmxObservation) (string, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	writer.UseCRLF = true

	if err := writer.Write(dataCSVHeader()); err != nil {
		return "", err
	}
	for _, observation := range obs {
		if observation == nil {
			continue
		}
		if len(observation.GetDatesAndValues()) == 0 {
			if err := writer.Write(f.row(observation, nil)); err != nil {
				return "", err
			}
			continue
		}
		for _, dateValue := range observation.GetDatesAndValues() {
			if err := writer.Write(f.row(observation, dateValue)); err != nil {
				return "", err
			}
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (f *CSVFormatter) row(observation *pb.SdmxObservation, dateValue *pb.SdmxDateValue) []string {
	row := []string{
		csvStructureDataflow,
		f.StructureID,
		csvActionInformation,
	}
	for _, component := range datacommons.DataComponents {
		row = append(row, dataCSVComponentValue(component, observation, dateValue))
	}
	return row
}

func dataCSVHeader() []string {
	header := []string{"STRUCTURE", "STRUCTURE_ID", "ACTION"}
	for _, component := range datacommons.DataComponents {
		header = append(header, component.ID)
	}
	return header
}

func dataCSVComponentValue(component datacommons.DataComponent, observation *pb.SdmxObservation, dateValue *pb.SdmxDateValue) string {
	value := dataCSVRawComponentValue(component.ID, observation, dateValue)
	if component.Kind == datacommons.ComponentKindDimension && value == "" {
		return datacommons.FallbackNotAvailable
	}
	return value
}

func dataCSVRawComponentValue(componentID string, observation *pb.SdmxObservation, dateValue *pb.SdmxDateValue) string {
	switch componentID {
	case datacommons.ComponentVariableMeasured:
		return observation.GetVariableMeasured()
	case datacommons.ComponentObservationAbout:
		return observation.GetDimensions()[datacommons.ComponentObservationAbout]
	case datacommons.ComponentUnit:
		return observation.GetAttributes()[datacommons.ComponentUnit]
	case datacommons.ComponentMeasurementMethod:
		return observation.GetAttributes()[datacommons.ComponentMeasurementMethod]
	case datacommons.ComponentObservationPeriod:
		return observation.GetAttributes()[datacommons.ComponentObservationPeriod]
	case datacommons.ComponentProvenance:
		return observation.GetProvenance()
	case datacommons.ComponentTimePeriod:
		if dateValue == nil {
			return ""
		}
		return dateValue.GetDate()
	case datacommons.ComponentObservationValue:
		if dateValue == nil {
			return ""
		}
		return dateValue.GetValue()
	case datacommons.ComponentScalingFactor:
		return observation.GetAttributes()[datacommons.ComponentScalingFactor]
	default:
		return ""
	}
}
