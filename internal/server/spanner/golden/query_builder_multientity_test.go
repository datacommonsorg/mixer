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

package golden

import (
	"context"
	"testing"

	"github.com/datacommonsorg/mixer/internal/server/spanner"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
)

func TestMultiEntityGetObservationsQuery(t *testing.T) {
	t.Parallel()

	for _, c := range multiEntityObservationsTestCases {
		c := c // Capture loop variable
		t.Run(c.name, func(t *testing.T) {
			goldenFile := c.golden + ".sql"
			runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
				return spanner.GetMultiEntityObservationsQuery(c.variables, c.entities, c.date)
			})
		})
	}
}

func TestMultiEntityGetStatVarsByEntityQuery(t *testing.T) {
	t.Parallel()

	for _, c := range multiEntityCheckVariableExistenceTestCases {
		c := c // Capture loop variable
		t.Run(c.name, func(t *testing.T) {
			goldenFile := c.golden + ".sql"
			runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
				return spanner.GetMultiEntityStatVarsByEntityQuery(c.variables, c.entities)
			})
		})
	}
}

func TestMultiEntityCheckGroupPlaceExistenceQuery(t *testing.T) {
	t.Parallel()

	for _, c := range multiEntityCheckGroupPlaceExistenceTestCases {
		c := c // Capture loop variable
		t.Run(c.name, func(t *testing.T) {
			goldenFile := c.golden + ".sql"
			runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
				return spanner.GetMultiEntityGroupPlaceExistenceQuery(c.variableGroups, c.entities, c.predicate), nil
			})
		})
	}
}

func TestMultiEntityGetObservationsContainedInPlaceQuery(t *testing.T) {
	t.Parallel()

	for _, c := range multiEntityObservationsContainedInPlaceTestCases {
		c := c // Capture loop variable
		t.Run(c.name, func(t *testing.T) {
			goldenFile := c.golden + ".sql"
			runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
				return spanner.GetMultiEntityObservationsContainedInPlaceQuery(c.variables, &v2.ContainedInPlace{
					Ancestor:       c.ancestor,
					ChildPlaceType: c.childPlaceType,
				}, c.date)
			})
		})
	}
}

func TestMultiEntityGetStatVarGroupNodeQuery(t *testing.T) {
	t.Parallel()

	for _, c := range multiEntityStatVarGroupNodeTestCases {
		c := c // Capture loop variable
		t.Run(c.name, func(t *testing.T) {
			goldenFile := c.golden + ".sql"
			runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
				return spanner.GetMultiEntityStatVarGroupNodeQuery(c.nodes, c.includeDefinitions), nil
			})
		})
	}
}

func TestMultiEntityGetFilteredSVGChildrenQuery(t *testing.T) {
	t.Parallel()

	for _, c := range multiEntityFilteredSVGChildrenTestCases {
		c := c // Capture loop variable
		t.Run(c.name, func(t *testing.T) {
			goldenFile := c.golden + ".sql"
			runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
				return spanner.GetMultiEntityFilteredSVGChildrenQuery(c.template, c.node, c.constrainedPlaces, c.constrainedProvenance, c.numEntitiesExistence, c.includeDefinitions), nil
			})
		})
	}
}

func TestMultiEntityGetFilteredTopicChildrenQuery(t *testing.T) {
	t.Parallel()

	for _, c := range multiEntityFilteredTopicTestCases {
		c := c // Capture loop variable
		t.Run(c.name, func(t *testing.T) {
			goldenFile := c.golden + ".sql"
			runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
				return spanner.GetMultiEntityFilteredTopicChildrenQuery(c.nodes, c.constrainedPlaces, c.constrainedProvenance, c.numEntitiesExistence), nil
			})
		})
	}
}
