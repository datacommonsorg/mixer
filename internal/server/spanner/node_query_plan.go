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
	"strings"

	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
)

type nodeQueryKind int

const (
	nodeQueryGeneric nodeQueryKind = iota
	nodeQueryContainedInPlace
)

type nodeQueryPlan struct {
	kind             nodeQueryKind
	containedInPlace containedInPlacePlan
}

type containedInPlacePlan struct {
	accessPath containedInPlaceAccessPath
}

func planNodeQuery(arc *v2.Arc, queryConfig QueryConfig) (nodeQueryPlan, error) {
	if arc == nil {
		return nodeQueryPlan{}, fmt.Errorf("node query arc must not be nil")
	}

	if childPlaceTypes, ok := matchNodeContainedInPlace(arc); ok {
		accessPath := containedInPlaceTypeFirst
		if len(childPlaceTypes) > 1 {
			accessPath = containedInPlaceAncestorFirst
		}
		return newNodeContainedInPlacePlan(accessPath), nil
	}

	if childPlaceTypes, ok := matchNodeLinkedContainedInPlace(arc); ok {
		return newNodeContainedInPlacePlan(
			queryConfig.containedInPlaceAccessPath(childPlaceTypes...),
		), nil
	}

	return nodeQueryPlan{kind: nodeQueryGeneric}, nil
}

func newNodeContainedInPlacePlan(accessPath containedInPlaceAccessPath) nodeQueryPlan {
	return nodeQueryPlan{
		kind: nodeQueryContainedInPlace,
		containedInPlace: containedInPlacePlan{
			accessPath: accessPath,
		},
	}
}

func matchNodeContainedInPlace(arc *v2.Arc) ([]string, bool) {
	return matchNodeContainmentProperty(arc, v2.ContainedInPlaceProperty)
}

func matchNodeLinkedContainedInPlace(arc *v2.Arc) ([]string, bool) {
	return matchNodeContainmentProperty(arc, linkedContainedInPlaceProperty)
}

func matchNodeContainmentProperty(arc *v2.Arc, property string) ([]string, bool) {
	if arc == nil ||
		arc.Out ||
		arc.SingleProp != property ||
		arc.Decorator != "" ||
		len(arc.BracketProps) > 0 ||
		len(arc.BracketFilters) > 0 ||
		len(arc.Filter) != 1 {
		return nil, false
	}
	values, ok := arc.Filter[predTypeOf]
	if !ok || len(values) == 0 {
		return nil, false
	}
	childPlaceTypes := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, childPlaceType := range values {
		if strings.TrimSpace(childPlaceType) == "" {
			return nil, false
		}
		if _, ok := seen[childPlaceType]; ok {
			continue
		}
		seen[childPlaceType] = struct{}{}
		childPlaceTypes = append(childPlaceTypes, childPlaceType)
	}
	return childPlaceTypes, true
}
