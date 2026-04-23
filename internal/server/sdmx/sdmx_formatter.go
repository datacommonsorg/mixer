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
	"encoding/json"
	"sort"
	"strconv"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
)

const (
	dimVariableMeasured = DimVariableMeasured
	dimObservationDate  = DimObservationDate
	dimProvenance       = "provenance"
)

// Formatter defines the interface for formatting SDMX query results.
type Formatter interface {
	Format(obs []*pb.SdmxObservation) (string, error)
}

// JSONStatFormatter implements Formatter for JSON-stat format.
type JSONStatFormatter struct {
	// TimeNow allows injecting a mock time for deterministic golden file testing.
	// If nil, time.Now is used.
	TimeNow func() time.Time
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
	Updated   string                    `json:"updated"`
	Id        []string                  `json:"id"`
	Size      []int                     `json:"size"`
	Dimension map[string]DimensionEntry `json:"dimension"`
	Value     []interface{}             `json:"value"`
	Extension map[string]interface{}    `json:"extension,omitempty"`
}

// Format converts Spanner observations into a full JSON-stat 2.0 string.
func (f *JSONStatFormatter) Format(obs []*pb.SdmxObservation) (string, error) {
	if len(obs) == 0 {
		return "{}", nil
	}

	dimensions, extensions := f.extractDimensions(obs)
	dimensionOrder, sortedCategories, size, strides, categoryIndices := f.computeStrides(dimensions)
	values := f.mapGridValues(obs, strides, categoryIndices, dimensionOrder)

	dimMap := map[string]DimensionEntry{}
	for _, dim := range dimensionOrder {
		dimMap[dim] = DimensionEntry{
			Label: dim,
			Category: Category{
				Index: sortedCategories[dim],
			},
		}
	}

	now := time.Now()
	if f.TimeNow != nil {
		now = f.TimeNow()
	}

	resp := JSONStatResponse{
		Version:   "2.0",
		Class:     "dataset",
		Label:     "Data Commons SDMX Query Results",
		Source:    "Data Commons",
		Updated:   now.Format(time.RFC3339),
		Id:        dimensionOrder,
		Size:      size,
		Dimension: dimMap,
		Value:     values,
		Extension: map[string]interface{}{
			"annotations": extensions,
		},
	}

	b, err := json.Marshal(resp)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (f *JSONStatFormatter) extractDimensions(obs []*pb.SdmxObservation) (map[string]map[string]bool, map[string]map[string]string) {
	dimensions := map[string]map[string]bool{}
	dimensions[dimVariableMeasured] = map[string]bool{}
	dimensions[dimObservationDate] = map[string]bool{}

	extensions := map[string]map[string]string{}

	for _, o := range obs {
		dimensions[dimVariableMeasured][o.VariableMeasured] = true
		for _, dv := range o.DatesAndValues {
			dimensions[dimObservationDate][dv.Date] = true
		}

		prov := o.Provenance
		if prov != "" && extensions[prov] == nil {
			extensions[prov] = map[string]string{}
		}

		for k, v := range o.Attributes {
			if prov != "" {
				extensions[prov][k] = v
			}
		}

		for k, v := range o.Dimensions {
			if _, ok := dimensions[k]; !ok {
				dimensions[k] = map[string]bool{}
			}
			dimensions[k][v] = true
		}
	}

	for _, o := range obs {
		for dim := range dimensions {
			if dim == dimVariableMeasured || dim == dimObservationDate || dim == dimProvenance {
				continue
			}
			if _, ok := o.Dimensions[dim]; !ok {
				dimensions[dim][FallbackNotAvailable] = true
			}
		}
	}

	return dimensions, extensions
}

func (f *JSONStatFormatter) computeStrides(dimensions map[string]map[string]bool) (
	dimensionOrder []string,
	sortedCategories map[string][]string,
	size []int,
	strides []int,
	categoryIndices map[string]map[string]int,
) {
	dimensionOrder = []string{dimVariableMeasured}
	middleDims := make([]string, 0, len(dimensions)-2)
	for dim := range dimensions {
		if dim != dimVariableMeasured && dim != dimObservationDate {
			middleDims = append(middleDims, dim)
		}
	}
	sort.Strings(middleDims)
	dimensionOrder = append(dimensionOrder, middleDims...)
	dimensionOrder = append(dimensionOrder, dimObservationDate)

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

	return dimensionOrder, sortedCategories, size, strides, categoryIndices
}

func (f *JSONStatFormatter) mapGridValues(
	obs []*pb.SdmxObservation,
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

	for _, o := range obs {
		varIdx := categoryIndices[dimVariableMeasured][o.VariableMeasured]
		baseIdx := varIdx * strides[0]

		for dimIdx, dim := range dimensionOrder {
			if dim == dimVariableMeasured || dim == dimObservationDate {
				continue
			}
			val, ok := o.Dimensions[dim]
			if !ok {
				val = FallbackNotAvailable
			}
			idx := categoryIndices[dim][val]
			baseIdx += idx * strides[dimIdx]
		}

		for _, dv := range o.DatesAndValues {
			dateIdx := categoryIndices[dimObservationDate][dv.Date]
			flatIdx := baseIdx + dateIdx*strides[len(dimensionOrder)-1]

			if fl, err := strconv.ParseFloat(dv.Value, 64); err == nil {
				values[flatIdx] = fl
			} else {
				values[flatIdx] = dv.Value
			}
		}
	}

	return values
}
