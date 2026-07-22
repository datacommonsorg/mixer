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

// ContainedInPlaceQueryConfig controls access-path selection for contained-in-place queries.
type ContainedInPlaceQueryConfig struct {
	// AncestorFirstTypes contains child place types that should filter by
	// ancestor before type. Other child place types filter by type first.
	AncestorFirstTypes []string
}

type containedInPlaceAccessPath int

const (
	containedInPlaceTypeFirst containedInPlaceAccessPath = iota
	containedInPlaceAncestorFirst
)

func validateAndCloneContainedInPlaceQueryConfig(config ContainedInPlaceQueryConfig) (ContainedInPlaceQueryConfig, error) {
	for _, placeType := range config.AncestorFirstTypes {
		if strings.TrimSpace(placeType) == "" {
			return ContainedInPlaceQueryConfig{}, fmt.Errorf("ContainedInPlaceQueryConfig: AncestorFirstTypes must not contain empty values")
		}
	}
	config.AncestorFirstTypes = slices.Clone(config.AncestorFirstTypes)
	return config, nil
}

func (config ContainedInPlaceQueryConfig) accessPath(childPlaceType string) containedInPlaceAccessPath {
	if slices.Contains(config.AncestorFirstTypes, childPlaceType) {
		return containedInPlaceAncestorFirst
	}
	return containedInPlaceTypeFirst
}
