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

package jsonstatv2

import (
	"encoding/json"
	"slices"
	"sort"
	"strconv"

	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	sdmxformat "github.com/datacommonsorg/mixer/internal/server/sdmx/format"
)

const (
	dimVariableMeasured = datacommons.ComponentVariableMeasured
	dimObservationDate  = datacommons.ComponentTimePeriod
	dimObservationValue = datacommons.ComponentObservationValue
	dimProvenance       = datacommons.ComponentProvenance

	jsonStatVersion = "2.0"
	jsonStatClass   = "dataset"
	defaultLabel    = "Data Commons SDMX Query Results"
	defaultSource   = "Data Commons"
	extAnnotations  = "annotations"
	extMeasures     = "measures"
)

// Formatter defines the interface for formatting SDMX query results.
type Formatter interface {
	Format(result *sdmxpb.SdmxDataResult) (string, error)
}

// JSONStatFormatter implements Formatter for JSON-stat format.
type JSONStatFormatter struct {
}

// Category represents JSON-stat category structure.
type Category struct {
	Index []string          `json:"index"`
	Label map[string]string `json:"label,omitempty"`
}

// DimensionEntry represents JSON-stat dimension entry.
type DimensionEntry struct {
	Label    string   `json:"label"`
	Category Category `json:"category"`
}

// JSONStatResponse represents full JSON-stat 2.0 response.
type JSONStatResponse struct {
	Version   string                    `json:"version"`
	Class     string                    `json:"class"`
	Label     string                    `json:"label"`
	Source    string                    `json:"source"`
	Id        []string                  `json:"id"`
	Size      []int                     `json:"size"`
	Dimension map[string]DimensionEntry `json:"dimension"`
	Value     []interface{}             `json:"value"`
	Extension map[string]interface{}    `json:"extension,omitempty"`
}

// Format converts a shape-driven SDMX result into a full JSON-stat 2.0 string.
func (f *JSONStatFormatter) Format(result *sdmxpb.SdmxDataResult) (string, error) {
	components, err := sdmxformat.DataComponentsFromShape(result.GetShape())
	if err != nil {
		return "", err
	}

	dimensions, dimensionOrder, extensions := f.extractDimensions(result.GetSeries(), components)
	sortedCategories, size, strides, categoryIndices := f.computeStrides(dimensions, dimensionOrder)
	values := f.mapGridValues(result.GetSeries(), strides, categoryIndices, dimensionOrder)

	dimMap := map[string]DimensionEntry{}
	for _, dim := range dimensionOrder {
		dimMap[dim] = DimensionEntry{
			Label: dim,
			Category: Category{
				Index: sortedCategories[dim],
			},
		}
	}

	resp := JSONStatResponse{
		Version:   jsonStatVersion,
		Class:     jsonStatClass,
		Label:     defaultLabel,
		Source:    defaultSource,
		Id:        dimensionOrder,
		Size:      size,
		Dimension: dimMap,
		Value:     values,
		Extension: map[string]interface{}{
			extAnnotations: extensions,
			extMeasures: map[string]map[string]string{
				dimObservationValue: {
					"label": dimObservationValue,
				},
			},
		},
	}

	b, err := json.Marshal(resp)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// extractDimensions identifies all unique values for each shape dimension.
func (f *JSONStatFormatter) extractDimensions(
	seriesList []*sdmxpb.SdmxTimeSeries,
	components []datacommons.DataComponent,
) (map[string]map[string]bool, []string, map[string]map[string]string) {
	dimensions := map[string]map[string]bool{}
	dimensionOrder := []string{}
	dimensionComponents := []datacommons.DataComponent{}
	for _, component := range components {
		if component.Kind != datacommons.ComponentKindDimension {
			continue
		}
		dimensions[component.ID] = map[string]bool{}
		dimensionOrder = append(dimensionOrder, component.ID)
		dimensionComponents = append(dimensionComponents, component)
	}
	extensions := map[string]map[string]string{}

	for _, series := range seriesList {
		if series == nil {
			continue
		}
		for _, component := range dimensionComponents {
			if component.ID == dimObservationDate {
				for _, point := range series.GetPoints() {
					value := sdmxformat.SdmxComponentValue(component, series, point)
					if value == "" {
						value = datacommons.FallbackNotAvailable
					}
					dimensions[component.ID][value] = true
				}
				continue
			}
			value := sdmxformat.SdmxComponentValue(component, series, nil)
			if value == "" {
				value = datacommons.FallbackNotAvailable
			}
			dimensions[component.ID][value] = true
		}

		prov := series.GetDimensions()[dimProvenance]
		if prov != "" && extensions[prov] == nil {
			extensions[prov] = map[string]string{}
		}

		for k, v := range series.GetAttributes() {
			if prov != "" {
				extensions[prov][k] = v
			}
		}
	}

	return dimensions, dimensionOrder, extensions
}

// computeStrides uses shape order for dimensions and sorts their values alphabetically.
// It then calculates the "strides" (step sizes) needed to map a multi-dimensional coordinate
// (e.g., [Location, Date, Variable]) into a single unique index in the flat, linear array
// that JSON-stat uses to store values.
func (f *JSONStatFormatter) computeStrides(
	dimensions map[string]map[string]bool,
	dimensionOrder []string,
) (
	sortedCategories map[string][]string,
	size []int,
	strides []int,
	categoryIndices map[string]map[string]int,
) {
	sortedCategories = map[string][]string{}
	for dim, values := range dimensions {
		var vals []string
		for v := range values {
			vals = append(vals, v)
		}
		sort.Strings(vals)
		sortedCategories[dim] = vals
	}

	size = []int{}
	totalSize := 1
	for _, dim := range dimensionOrder {
		sz := len(sortedCategories[dim])
		size = append(size, sz)
		totalSize *= sz
	}
	// TODO for Production: Add protective capacity limit to prevent OOM crashes when dimensional combination matrix exceeds a safe threshold (e.g., 100,000 cells).

	strides = make([]int, len(dimensionOrder))
	stride := 1
	for i := len(dimensionOrder) - 1; i >= 0; i-- {
		strides[i] = stride
		stride *= len(sortedCategories[dimensionOrder[i]])
	}

	categoryIndices = map[string]map[string]int{}
	for dim, vals := range sortedCategories {
		categoryIndices[dim] = map[string]int{}
		for idx, val := range vals {
			categoryIndices[dim][val] = idx
		}
	}

	return sortedCategories, size, strides, categoryIndices
}

// mapGridValues places each observation's value into its correct spot in the final flat array.
// It uses the sorted value indices and computed strides to calculate the exact 1D position
// for each combination of dimension values.
func (f *JSONStatFormatter) mapGridValues(
	seriesList []*sdmxpb.SdmxTimeSeries,
	strides []int,
	categoryIndices map[string]map[string]int,
	dimensionOrder []string,
) []interface{} {
	totalSize := 1
	for _, dim := range dimensionOrder {
		totalSize *= len(categoryIndices[dim])
	}

	values := make([]interface{}, totalSize)
	for i := range values {
		values[i] = nil
	}

	timePeriodDimIdx := slices.Index(dimensionOrder, dimObservationDate)
	if timePeriodDimIdx < 0 {
		return values
	}

	for _, series := range seriesList {
		if series == nil {
			continue
		}
		baseIdx := 0
		for dimIdx, dim := range dimensionOrder {
			if dim == dimObservationDate {
				continue
			}
			val := series.GetDimensions()[dim]
			if val == "" {
				val = datacommons.FallbackNotAvailable
			}
			idx := categoryIndices[dim][val]
			baseIdx += idx * strides[dimIdx]
		}

		for _, point := range series.GetPoints() {
			date := point.GetTimePeriod()
			if date == "" {
				date = datacommons.FallbackNotAvailable
			}
			dateIdx := categoryIndices[dimObservationDate][date]
			flatIdx := baseIdx + dateIdx*strides[timePeriodDimIdx]

			if fl, err := strconv.ParseFloat(point.GetObservationValue(), 64); err == nil {
				values[flatIdx] = fl
			} else {
				values[flatIdx] = point.GetObservationValue()
			}
		}
	}

	return values
}
