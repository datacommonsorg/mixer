// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spanner

import (
	"fmt"
	"slices"
	"strings"
)

// QueryConfig controls Spanner query-planning behavior.
type QueryConfig struct {
	// ContainedInPlaceAncestorFirstTypes contains child place types that
	// should filter by ancestor before type when the query builder supports
	// that access path.
	ContainedInPlaceAncestorFirstTypes []string
	// ContainedInPlaceEntityScanMinVariables is the minimum number of unique
	// requested variables that selects the entity1 range-scan plan for core
	// contained-in-place queries. Zero disables the optimization.
	ContainedInPlaceEntityScanMinVariables int
}

type containedInPlaceAccessPath int

const (
	containedInPlaceTypeFirst containedInPlaceAccessPath = iota
	containedInPlaceAncestorFirst
)

// Validate verifies that QueryConfig is safe to distribute to query builders.
func (config QueryConfig) Validate() error {
	if config.ContainedInPlaceEntityScanMinVariables < 0 {
		return fmt.Errorf("QueryConfig: ContainedInPlaceEntityScanMinVariables must be non-negative")
	}
	for _, placeType := range config.ContainedInPlaceAncestorFirstTypes {
		trimmed := strings.TrimSpace(placeType)
		if trimmed == "" {
			return fmt.Errorf("QueryConfig: ContainedInPlaceAncestorFirstTypes must not contain empty values")
		}
		if trimmed != placeType {
			return fmt.Errorf("QueryConfig: ContainedInPlaceAncestorFirstTypes must not contain surrounding whitespace")
		}
	}
	return nil
}

func (config QueryConfig) containedInPlaceAccessPath(childPlaceTypes ...string) containedInPlaceAccessPath {
	for _, childPlaceType := range childPlaceTypes {
		if slices.Contains(config.ContainedInPlaceAncestorFirstTypes, childPlaceType) {
			return containedInPlaceAncestorFirst
		}
	}
	return containedInPlaceTypeFirst
}
