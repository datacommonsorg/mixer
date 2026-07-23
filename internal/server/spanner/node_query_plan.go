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
	kind       nodeQueryKind
	accessPath containedInPlaceAccessPath
}

func planNodeQuery(arc *v2.Arc, queryConfig QueryConfig) (nodeQueryPlan, error) {
	if arc == nil {
		return nodeQueryPlan{}, fmt.Errorf("node query arc must not be nil")
	}

	childPlaceType, ok := matchNodeContainedInPlace(arc)
	if !ok {
		return nodeQueryPlan{kind: nodeQueryGeneric}, nil
	}
	return nodeQueryPlan{
		kind:       nodeQueryContainedInPlace,
		accessPath: queryConfig.containedInPlaceAccessPath(childPlaceType),
	}, nil
}

func matchNodeContainedInPlace(arc *v2.Arc) (string, bool) {
	if arc == nil ||
		arc.Out ||
		arc.SingleProp != linkedContainedInPlaceProperty ||
		arc.Decorator != "" ||
		len(arc.BracketProps) > 0 ||
		len(arc.BracketFilters) > 0 {
		return "", false
	}
	return singleFilterValue(arc.Filter, predTypeOf)
}

func singleFilterValue(filters map[string][]string, property string) (string, bool) {
	if len(filters) != 1 {
		return "", false
	}
	values, ok := filters[property]
	if !ok || len(values) != 1 || strings.TrimSpace(values[0]) == "" {
		return "", false
	}
	return values[0], true
}
