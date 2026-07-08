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

	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	sdmxformat "github.com/datacommonsorg/mixer/internal/server/sdmx/format"
)

const (
	csvStructureDataflow = "dataflow"
	csvActionInformation = "I"
)

// CSVFormatter implements Formatter for SDMX-CSV 2.0 data messages.
type CSVFormatter struct {
	StructureID string
}

// Format converts a shape-driven SDMX result into a complete SDMX-CSV 2.0 payload.
func (f *CSVFormatter) Format(result *sdmxpb.SdmxDataResult) (string, error) {
	components, err := sdmxformat.DataComponentsFromShape(result.GetShape())
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	writer.UseCRLF = true

	if err := writer.Write(dataCSVHeader(components)); err != nil {
		return "", err
	}
	for _, series := range result.GetSeries() {
		if series == nil {
			continue
		}
		if len(series.GetPoints()) == 0 {
			if err := writer.Write(f.row(components, series, nil)); err != nil {
				return "", err
			}
			continue
		}
		for _, point := range series.GetPoints() {
			if err := writer.Write(f.row(components, series, point)); err != nil {
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

func (f *CSVFormatter) row(
	components []datacommons.DataComponent,
	series *sdmxpb.SdmxTimeSeries,
	point *sdmxpb.SdmxDataPoint,
) []string {
	row := []string{
		csvStructureDataflow,
		f.StructureID,
		csvActionInformation,
	}
	for _, component := range components {
		row = append(row, dataCSVComponentValue(component, series, point))
	}
	return row
}

func dataCSVHeader(components []datacommons.DataComponent) []string {
	header := []string{"STRUCTURE", "STRUCTURE_ID", "ACTION"}
	for _, component := range components {
		header = append(header, component.ID)
	}
	return header
}

func dataCSVComponentValue(
	component datacommons.DataComponent,
	series *sdmxpb.SdmxTimeSeries,
	point *sdmxpb.SdmxDataPoint,
) string {
	value := sdmxformat.SdmxComponentValue(component, series, point)
	if component.Kind == datacommons.ComponentKindDimension && value == "" {
		return datacommons.FallbackNotAvailable
	}
	return value
}
