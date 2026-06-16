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

package sdmx

import (
	"bytes"
	"encoding/csv"

	pb "github.com/datacommonsorg/mixer/internal/proto"
)

const (
	csvStructureDataflow = "dataflow"
	csvActionInformation = "I"
)

var dataCSVHeader = []string{
	"STRUCTURE",
	"STRUCTURE_ID",
	"ACTION",
	DimVariableMeasured,
	"observationAbout",
	"unit",
	"measurementMethod",
	"observationPeriod",
	"provenance",
	DimObservationDate,
	DimObservationValue,
	"scalingFactor",
}

// CSVFormatter implements Formatter for SDMX-CSV 2.0 data messages.
type CSVFormatter struct {
	StructureID string
}

// Format converts observations into a complete SDMX-CSV 2.0 payload.
func (f *CSVFormatter) Format(obs []*pb.SdmxObservation) (string, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	writer.UseCRLF = true

	if err := writer.Write(dataCSVHeader); err != nil {
		return "", err
	}
	for _, observation := range obs {
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
	date := ""
	value := ""
	if dateValue != nil {
		date = dateValue.GetDate()
		value = dateValue.GetValue()
	}

	return []string{
		csvStructureDataflow,
		f.StructureID,
		csvActionInformation,
		observation.GetVariableMeasured(),
		observation.GetDimensions()["observationAbout"],
		observation.GetAttributes()["unit"],
		observation.GetAttributes()["measurementMethod"],
		observation.GetAttributes()["observationPeriod"],
		observation.GetProvenance(),
		date,
		value,
		observation.GetAttributes()["scalingFactor"],
	}
}
